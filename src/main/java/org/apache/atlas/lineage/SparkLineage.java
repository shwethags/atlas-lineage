/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SparkLineage {
    //atlas entity attributes
    public static final String SPARK_PROCESS = "spark_process";
    public static final String HIVE_TABLE = "hive_table";
    public static final String APP_VERSION = "appVersion";
    public static final String APP = "app";
    public static final String LIBRARY_DEPENDENCIES = "libraryDependencies";
    private static final String JOB_ID = "jobId";
    private static final String QUERY_TEXT = "queryText";

    //cli options
    private static final String ATLAS_ENDPOINT = "a";
    private static final String CLUSTER_NAME = "c";
    private static final String SPARK_JOB_ID = "j";
    private static final String SPARK_PROCESS_NAME = "p";

    private final AtlasClient atlasClient;
    private final String clusterName;

    public static final void main(String args[]) throws Exception {
        Options options = configureOptions();
        CommandLine cmdLine = null;
        try {
            CommandLineParser parser = new GnuParser();
            cmdLine = parser.parse(options, args);
            SparkLineage test = new SparkLineage(cmdLine.getOptionValue(ATLAS_ENDPOINT),
            cmdLine.getOptionValue(CLUSTER_NAME));
            test.createTypes();
            test.createLineage(cmdLine.getOptionValue(SPARK_JOB_ID), cmdLine.getOptionValue(SPARK_PROCESS_NAME));
        } catch(Exception e) {
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hadoop  jar atlas-lineage-0.1.jar org.apache.atlas.lineage.SparkLineage", options );
        }
    }

    public SparkLineage(String endpoint, String clusterName) {
        atlasClient = new AtlasClient(new String[]{endpoint}, new String[]{"admin", "admin"});
        this.clusterName = clusterName;
    }

    private void createLineage(String jobId, String processName) throws AtlasServiceException {
        List<Id> inIds = new ArrayList<>();
        String[] intables = {"employees", "departments", "dept_emp"};
        for (String table : intables) {
            String qname = "employees." + table + "@" + clusterName;
            String id = atlasClient.getEntity(HIVE_TABLE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qname).getId()._getId();
            inIds.add(new Id(id, 1, HIVE_TABLE));
        }

        String qname = "default.emp_dept_flat@" + clusterName;
        String outid = atlasClient.getEntity(HIVE_TABLE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qname).getId()._getId();

        Referenceable lineage = new Referenceable(SPARK_PROCESS);
        lineage.set(AtlasClient.PROCESS_ATTRIBUTE_INPUTS, inIds);
        lineage.set(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS, Arrays.asList(new Id(outid, 1, HIVE_TABLE)));
        lineage.set(AtlasClient.NAME, processName);
        lineage.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processName);
        lineage.set(AtlasClient.OWNER, "etl");
        lineage.set(AtlasClient.DESCRIPTION, "spark etl job to generate emp_dept_flat");
        lineage.set(APP_VERSION, "1.0");
        String app = "import org.apache.log4j.{Level, Logger}\n<br/>" +
                "import org.apache.spark.sql.DataFrame\n<br/>" +
                "\n<br/>" +
                "val rootLogger = Logger.getRootLogger()\n<br/>" +
                "rootLogger.setLevel(Level.ERROR)\n<br/>" +
                "\n<br/>" +
                "\n<br/>" +
                "def saveToHive(df: DataFrame, databaseName: String, tableName: String) = {\n<br/>" +
                "    val tempTable = s\"${tableName}_tmp_${System.currentTimeMillis / 1000}\"\n<br/>" +
                "    df.registerTempTable(tempTable)\n<br/>" +
                "    sqlContext.sql(s\"create table ${databaseName}.${tableName} stored as ORC as select * from ${tempTable}\")\n<br/>" +
                "    sqlContext.dropTempTable(tempTable)\n<br/>" +
                "\n<br/>" +
                "}\n<br/>" +
                "\n<br/>" +
                "val employees = sqlContext.sql(\"select * from employees.employees\")\n<br/>" +
                "val departments = sqlContext.sql(\"select * from employees.departments\")\n<br/>" +
                "val dept_emp = sqlContext.sql(\"select * from employees.dept_emp\")\n<br/>" +
                "\n<br/>" +
                "val flat = employees.withColumn(\"full_name\", concat(employees(\"last_name\"), lit(\", \"), employees(\"first_name\"))).\n<br/>" +
                "                     select(\"full_name\", \"emp_no\").\n<br/>" +
                "                     join(dept_emp,\"emp_no\").\n<br/>" +
                "                     join(departments, \"dept_no\")\n<br/>" +
                "flat.show()\n<br/>" +
                "\n<br/>" +
                "\n<br/>" +
                "saveToHive(flat, \"default\", \"emp_dept_flat\")<br/>";
        lineage.set(APP, "etl.scala");
        lineage.set(QUERY_TEXT, app);
        lineage.set(LIBRARY_DEPENDENCIES, "\"org.apache.spark\" %% \"spark-core\" % \"2.1.0\"");
        lineage.set(JOB_ID, jobId);

        try {
            Referenceable entity = atlasClient.getEntity(SPARK_PROCESS, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processName);
            atlasClient.updateEntity(entity.getId()._getId(), lineage);
            System.out.printf("updated id " + entity.getId()._getId());
        } catch (AtlasServiceException e) {
            List<String> id = atlasClient.createEntity(lineage);
            System.out.println("created ids " + id);
        }

//        System.out.printf("Submitting instance = " + InstanceSerialization.toJson(lineage, true));
    }

    private void createTypes() throws AtlasServiceException {
        HierarchicalTypeDefinition<ClassType> clsDef = TypesUtil
                .createClassTypeDef(SPARK_PROCESS, "spark process", ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE),
                        attrDef(APP_VERSION, DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef(APP, DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef(JOB_ID, DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef(QUERY_TEXT, DataTypes.STRING_TYPE, Multiplicity.OPTIONAL),
                        attrDef(LIBRARY_DEPENDENCIES, DataTypes.arrayTypeName(DataTypes.STRING_TYPE)));

        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(clsDef));

        String typesAsJSON = TypesSerialization.toJson(typesDef);
        System.out.println("typesAsJSON = " + typesAsJSON);
        if (atlasClient.getType(SPARK_PROCESS) == null) {
            System.out.println("Creating type");
            atlasClient.createType(typesAsJSON);
        } else {
            System.out.println("Updating type");
            atlasClient.updateType(typesAsJSON);
        }
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m) {
        return attrDef(name, dT, m, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m, boolean isComposite,
            String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT.getName(), m, isComposite, false, false, reverseAttributeName);
    }

    AttributeDefinition attrDef(String name, String typeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(typeName);
        return new AttributeDefinition(name, typeName, Multiplicity.OPTIONAL, false, false, false, null);
    }

    private static Options configureOptions() {
        Options options = new Options();
        options.addOption(ATLAS_ENDPOINT.toString(), "atlas", true, "Atlas endpoint");
        options.addOption(CLUSTER_NAME.toString(), "cluster", true, "HDP cluster name");
        options.addOption(SPARK_JOB_ID.toString(), "job", true, "Spark job id");
        options.addOption(SPARK_PROCESS_NAME.toString(), "process", true, "Spark process name");
        return options;
    }
}
