/*
 *
 *  * Copyright 2023 Google LLC
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher;

import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScanner;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BigQueryScopeLister {

    private final ResourceScanner resourceScanner;
    private LoggingHelper logger;
    private final String runId;

    private static final String REGEX_PREFIX = "regex:";

    public BigQueryScopeLister(ResourceScanner resourceScanner,
                               LoggingHelper logger,
                               String runId) {
        this.resourceScanner = resourceScanner;
        this.logger = logger;
        this.runId = runId;
    }

    /**
     * List and filer down all tables that should be in scope based on BigQueryScope object
     * <p>
     * Detecting which resources to list is done bottom up TABLES > DATASETS > PROJECTS > FOLDERS
     * where lower levels configs (e.g. Tables) ignore higher level configs (e.g. Datasets)
     * For example:
     * If TABLES_INCLUDE list is provided:
     * * List only these tables
     * * SKIP tables in TABLES_EXCLUDE list
     * * IGNORE all other INCLUDE lists
     * If DATASETS_INCLUDE list is provided:
     * * List only tables in these datasets
     * * SKIP datasets in DATASETS_EXCLUDE
     * * SKIP tables in TABLES_EXCLUDE
     * * IGNORE all other INCLUDE lists
     * If PROJECTS_INCLUDE list is provided:
     * * List only datasets and tables in these projects
     * * SKIP datasets in DATASETS_EXCLUDE
     * * SKIP tables in TABLES_EXCLUDE
     * * IGNORE all other INCLUDE lists
     * If FOLDERS_INCLUDE list is provided:
     * * List only projects, datasets and tables in these folders
     * * SKIP projects in PROJECTS_EXCLUDE
     * * SKIP datasets in DATASETS_EXCLUDE
     * * SKIP tables in TABLES_EXCLUDE
     * * IGNORE all other INCLUDE lists
     *
     * @param bqScope
     * @return List<TableSpec>
     * @throws NonRetryableApplicationException
     */
    public List<TableSpec> listTablesInScope(BigQueryScope bqScope) throws NonRetryableApplicationException {

        List<TableSpec> tablesInScope;

        // Pre-compile all regular expressions in exclude lists to improve performance
        List<Pattern> tableExcludeListPatterns = extractAndCompilePatterns(bqScope.getTableExcludeList(), REGEX_PREFIX);
        List<Pattern> datasetExcludeListPatterns = extractAndCompilePatterns(bqScope.getDatasetExcludeList(), REGEX_PREFIX);
        List<Pattern> projectExcludeListPatterns = extractAndCompilePatterns(bqScope.getProjectExcludeList(), REGEX_PREFIX);

        if (!bqScope.getTableIncludeList().isEmpty()) {
            tablesInScope = processTables(
                    bqScope.getTableIncludeList(),
                    bqScope.getTableExcludeList(),
                    tableExcludeListPatterns
            );
        } else {

            if (!bqScope.getDatasetIncludeList().isEmpty()) {
                tablesInScope = processDatasets(
                        bqScope.getDatasetIncludeList(),
                        bqScope.getDatasetExcludeList(),
                        bqScope.getTableExcludeList(),
                        datasetExcludeListPatterns,
                        tableExcludeListPatterns
                );
            } else {
                if (!bqScope.getProjectIncludeList().isEmpty()) {
                    tablesInScope = processProjects(
                            bqScope.getProjectIncludeList(),
                            bqScope.getProjectExcludeList(),
                            bqScope.getDatasetExcludeList(),
                            bqScope.getTableExcludeList(),
                            projectExcludeListPatterns,
                            datasetExcludeListPatterns,
                            tableExcludeListPatterns
                    );
                } else {
                    if (!bqScope.getFolderIncludeList().isEmpty()) {
                        tablesInScope = processFolders(
                                bqScope.getFolderIncludeList(),
                                bqScope.getProjectExcludeList(),
                                bqScope.getDatasetExcludeList(),
                                bqScope.getTableExcludeList(),
                                projectExcludeListPatterns,
                                datasetExcludeListPatterns,
                                tableExcludeListPatterns
                        );
                    } else {
                        throw new NonRetryableApplicationException("At least one of of the following params must be not empty [tableIncludeList, datasetIncludeList, projectIncludeList, folderIncludeList]");
                    }
                }
            }
        }

        return tablesInScope;
    }

    private List<TableSpec> processTables(List<String> tableIncludeList,
                                          List<String> tableExcludeList,
                                          List<Pattern> tableExcludeListPatterns
    ) {
        List<TableSpec> output = new ArrayList<>();

        for (String table : tableIncludeList) {
            TableSpec tableSpec = TableSpec.fromSqlString(table);
            try {
                Tuple<Boolean, String> checkResults = isIncluded(table, tableExcludeList, tableExcludeListPatterns);
                if (!checkResults.x()) {
                    output.add(tableSpec);
                } else {
                    logger.logInfoWithTracker(runId, tableSpec, String.format("Table %s is excluded by %s", table, checkResults.y()));
                }
            } catch (Exception ex) {
                // log and continue
                logger.logFailedDispatcherEntityId(runId, tableSpec, table, ex.getMessage(), ex.getClass().getName());
            }
        }
        return output;
    }

    private List<TableSpec> processDatasets(List<String> datasetIncludeList,
                                            List<String> datasetExcludeList,
                                            List<String> tableExcludeList,
                                            List<Pattern> datasetExcludeListPatterns,
                                            List<Pattern> tableExcludeListPatterns
    ) {

        List<String> tablesIncludeList = new ArrayList<>();

        for (String dataset : datasetIncludeList) {

            try {

                Tuple<Boolean, String> checkResults = isIncluded(dataset, datasetExcludeList, datasetExcludeListPatterns);

                if (!checkResults.x()) {

                    List<String> tokens = Utils.tokenize(dataset, ".", true);
                    String projectId = tokens.get(0);
                    String datasetId = tokens.get(1);

                    // get all tables under dataset
                    List<String> datasetTables = resourceScanner.listTables(projectId, datasetId);
                    tablesIncludeList.addAll(datasetTables);

                    if (datasetTables.isEmpty()) {
                        String msg = String.format(
                                "No Tables found under dataset '%s'",
                                dataset);

                        logger.logWarnWithTracker(runId, null, msg);
                    } else {
                        logger.logInfoWithTracker(runId, null, String.format("Found %s tables under dataset %s", datasetTables.size(), dataset));
                    }
                } else {
                    logger.logInfoWithTracker(runId, null, String.format("Dataset %s is excluded by %s", dataset, checkResults.y()));
                }
            } catch (Exception exception) {
                // log and continue
                logger.logFailedDispatcherEntityId(runId, null, dataset, exception.getMessage(), exception.getClass().getName());
            }
        }
        return processTables(
                tablesIncludeList,
                tableExcludeList,
                tableExcludeListPatterns);
    }


    private List<TableSpec> processProjects(
            List<String> projectIncludeList,
            List<String> projectExcludeList,
            List<String> datasetExcludeList,
            List<String> tableExcludeList,
            List<Pattern> projectExcludeListPatterns,
            List<Pattern> datasetExcludeListPatterns,
            List<Pattern> tableExcludeListPatterns
    ) {

        List<String> datasetIncludeList = new ArrayList<>();

        logger.logInfoWithTracker(runId, null, String.format("Will process projects %s", projectIncludeList));

        for (String project : projectIncludeList) {
            try {

                Tuple<Boolean, String> checkResults = isIncluded(project, projectExcludeList, projectExcludeListPatterns);

                if (!checkResults.x()) {

                    logger.logInfoWithTracker(runId, null, String.format("Inspecting project %s", project));

                    // get all datasets in this project
                    List<String> projectDatasets = resourceScanner.listDatasets(project);
                    datasetIncludeList.addAll(projectDatasets);

                    if (projectDatasets.isEmpty()) {
                        String msg = String.format(
                                "No datasets found under project '%s' or no enough permissions to list BigQuery resources.",
                                project);

                        logger.logWarnWithTracker(runId, null, msg);
                    } else {

                        logger.logInfoWithTracker(runId, null, String.format("Datasets found in project %s : %s", project, projectDatasets));
                    }
                } else {
                    logger.logInfoWithTracker(runId, null, String.format("Project %s is excluded by %s", project, checkResults.y()));
                }

            } catch (Exception exception) {
                // log and continue
                logger.logFailedDispatcherEntityId(runId, null, project, exception.getMessage(), exception.getClass().getName());
            }

        }
        return processDatasets(
                datasetIncludeList,
                datasetExcludeList,
                tableExcludeList,
                datasetExcludeListPatterns,
                tableExcludeListPatterns);
    }

    private List<TableSpec> processFolders(
            List<Long> folderIncludeList,
            List<String> projectExcludeList,
            List<String> datasetExcludeList,
            List<String> tableExcludeList,
            List<Pattern> projectExcludeListPatterns,
            List<Pattern> datasetExcludeListPatterns,
            List<Pattern> tableExcludeListPatterns
    ) {

        List<String> projectIncludeList = new ArrayList<>();

        logger.logInfoWithTracker(runId, null, String.format("Will process folders %s", folderIncludeList));

        for (Long folder : folderIncludeList) {
            try {

                logger.logInfoWithTracker(runId, null, String.format("Inspecting folder %s", folder));

                // get all projects in this folder
                List<String> folderProjects = resourceScanner.listProjects(folder);
                projectIncludeList.addAll(folderProjects);

                if (folderProjects.isEmpty()) {
                    String msg = String.format(
                            "No projects found under folder '%s' or no enough permissions to list.",
                            folder);

                    logger.logWarnWithTracker(runId, null, msg);
                } else {

                    logger.logInfoWithTracker(runId, null, String.format("Found %s projects under folder %s", folderProjects.size(), folder));
                }

            } catch (Exception exception) {
                // log and continue
                logger.logFailedDispatcherEntityId(runId, null, folder.toString(), exception.getMessage(), exception.getClass().getName());
            }

        }
        return processProjects(
                projectIncludeList,
                projectExcludeList,
                datasetExcludeList,
                tableExcludeList,
                projectExcludeListPatterns,
                datasetExcludeListPatterns,
                tableExcludeListPatterns);
    }

    private Tuple<Boolean, String> isIncluded(String input, List<String> list, List<Pattern> patterns) {

        // check if the input matches any regex
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(input);
            if (matcher.find()) {
                return Tuple.of(true, pattern.toString());
            }
        }

        // check if the input matches any literal element in the list
        for (String listElement : list) {
            // check if the input matches any literal element in the list
            if (listElement.equalsIgnoreCase(input)) {
                return Tuple.of(true, listElement);
            }
        }

        // if the input is not found, return false and no matching elements
        return Tuple.of(false, null);
    }

    /**
     * Extract elements starting with a certain prefix, compile them and return them in a new List
     *
     * @param list
     * @return List of compiled regular expression patterns
     */
    private static List<Pattern> extractAndCompilePatterns(List<String> list, String prefix) {
        List<Pattern> patterns = new ArrayList<>();
        for (String element : list) {
            if (element.toLowerCase().startsWith(prefix)) {
                String regex = element.substring(prefix.length());
                patterns.add(Pattern.compile(regex));
            }
        }
        return patterns;
    }

}
