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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher;


import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.DatasetSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.GlobalVariables;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScanner;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class BigQueryScopeLister {

  private final ResourceScanner resourceScanner;
  private LoggingHelper logger;
  private final String runId;

  private static final String REGEX_PREFIX = GlobalVariables.REGEX_PREFIX;

  public BigQueryScopeLister(ResourceScanner resourceScanner, LoggingHelper logger, String runId) {
    this.resourceScanner = resourceScanner;
    this.logger = logger;
    this.runId = runId;
  }

  /**
   * List and filer down all datasets that should be in scope based on BigQueryScope object
   *
   * <p>Detecting which resources to list is done bottom up DATASETS > PROJECTS > FOLDERS where
   * lower levels configs (e.g. Datasets) ignore higher level configs (e.g. Projects) For example:
   * If DATASETS_INCLUDE list is provided: * List only these datasets * SKIP datasets in
   * DATASETS_EXCLUDE * SKIP tables in TABLES_EXCLUDE * IGNORE other INCLUDE lists If
   * PROJECTS_INCLUDE list is provided: * List only datasets and tables in these projects * SKIP
   * datasets in DATASETS_EXCLUDE * SKIP tables in TABLES_EXCLUDE * IGNORE other INCLUDE lists If
   * FOLDERS_INCLUDE list is provided: * List only projects, datasets and tables in these folders *
   * SKIP projects in PROJECTS_EXCLUDE * SKIP datasets in DATASETS_EXCLUDE * SKIP tables in
   * TABLES_EXCLUDE * IGNORE all other INCLUDE lists
   *
   * @param bqScope
   * @return List<TableSpec>
   * @throws NonRetryableApplicationException
   */
  public List<DatasetSpec> listDatasetsInScope(BigQueryScope bqScope)
      throws NonRetryableApplicationException {

    List<DatasetSpec> datasetsInScope;

    // Pre-compile all regular expressions in exclude lists to improve performance
    List<Pattern> datasetExcludeListPatterns =
        Utils.extractAndCompilePatterns(bqScope.getDatasetExcludeList(), REGEX_PREFIX);
    List<Pattern> projectExcludeListPatterns =
        Utils.extractAndCompilePatterns(bqScope.getProjectExcludeList(), REGEX_PREFIX);

    if (!bqScope.getDatasetIncludeList().isEmpty()) {
      datasetsInScope =
          processDatasets(
              bqScope.getDatasetIncludeList(),
              bqScope.getDatasetExcludeList(),
              datasetExcludeListPatterns);
    } else {
      if (!bqScope.getProjectIncludeList().isEmpty()) {
        datasetsInScope =
            processProjects(
                bqScope.getProjectIncludeList(),
                bqScope.getProjectExcludeList(),
                bqScope.getDatasetExcludeList(),
                projectExcludeListPatterns,
                datasetExcludeListPatterns);
      } else {
        if (!bqScope.getFolderIncludeList().isEmpty()) {
          datasetsInScope =
              processFolders(
                  bqScope.getFolderIncludeList(),
                  bqScope.getProjectExcludeList(),
                  bqScope.getDatasetExcludeList(),
                  projectExcludeListPatterns,
                  datasetExcludeListPatterns);
        } else {
          throw new NonRetryableApplicationException(
              "At least one of of the following params must be not empty [tableIncludeList,"
                  + " datasetIncludeList, projectIncludeList, folderIncludeList]");
        }
      }
    }

    return datasetsInScope;
  }

  private List<DatasetSpec> processDatasets(
      List<String> datasetIncludeList,
      List<String> datasetExcludeList,
      List<Pattern> datasetExcludeListPatterns) {

    List<DatasetSpec> datasetsInScope = new ArrayList<>();

    for (String dataset : datasetIncludeList) {

      try {

        Tuple<Boolean, String> checkResults =
            Utils.isElementMatchLiteralOrRegexList(
                dataset, datasetExcludeList, datasetExcludeListPatterns);

        if (!checkResults.x()) {

          List<String> tokens = Utils.tokenize(dataset, ".", true);
          String projectId = tokens.get(0);
          String datasetId = tokens.get(1);

          datasetsInScope.add(new DatasetSpec(projectId, datasetId));

        } else {
          logger.logInfoWithTracker(
              runId,
              null,
              String.format("Dataset %s is excluded by %s", dataset, checkResults.y()));
        }
      } catch (Exception exception) {
        // log and continue
        logger.logFailedDispatcherEntityId(
            runId, null, dataset, exception.getMessage(), exception.getClass().getName());
      }
    }
    return datasetsInScope;
  }

  private List<DatasetSpec> processProjects(
      List<String> projectIncludeList,
      List<String> projectExcludeList,
      List<String> datasetExcludeList,
      List<Pattern> projectExcludeListPatterns,
      List<Pattern> datasetExcludeListPatterns) {

    List<String> datasetIncludeList = new ArrayList<>();

    logger.logInfoWithTracker(
        runId, null, String.format("Will process projects %s", projectIncludeList));

    for (String project : projectIncludeList) {
      try {

        Tuple<Boolean, String> checkResults =
            Utils.isElementMatchLiteralOrRegexList(
                project, projectExcludeList, projectExcludeListPatterns);

        if (!checkResults.x()) {

          logger.logInfoWithTracker(runId, null, String.format("Inspecting project %s", project));

          // get all datasets in this project
          List<String> projectDatasets = resourceScanner.listDatasets(project);
          datasetIncludeList.addAll(projectDatasets);

          if (projectDatasets.isEmpty()) {
            String msg =
                String.format(
                    "No datasets found under project '%s' or no enough permissions to list BigQuery"
                        + " resources.",
                    project);

            logger.logWarnWithTracker(runId, null, msg);
          } else {

            logger.logInfoWithTracker(
                runId,
                null,
                String.format("Datasets found in project %s : %s", project, projectDatasets));
          }
        } else {
          logger.logInfoWithTracker(
              runId,
              null,
              String.format("Project %s is excluded by %s", project, checkResults.y()));
        }

      } catch (Exception exception) {
        // log and continue
        logger.logFailedDispatcherEntityId(
            runId, null, project, exception.getMessage(), exception.getClass().getName());
      }
    }
    return processDatasets(datasetIncludeList, datasetExcludeList, datasetExcludeListPatterns);
  }

  private List<DatasetSpec> processFolders(
      List<Long> folderIncludeList,
      List<String> projectExcludeList,
      List<String> datasetExcludeList,
      List<Pattern> projectExcludeListPatterns,
      List<Pattern> datasetExcludeListPatterns) {

    List<String> projectIncludeList = new ArrayList<>();

    logger.logInfoWithTracker(
        runId, null, String.format("Will process folders %s", folderIncludeList));

    for (Long folder : folderIncludeList) {
      try {

        logger.logInfoWithTracker(runId, null, String.format("Inspecting folder %s", folder));

        // get all projects in this folder
        List<String> folderProjects = resourceScanner.listProjects(folder);
        projectIncludeList.addAll(folderProjects);

        if (folderProjects.isEmpty()) {
          String msg =
              String.format(
                  "No projects found under folder '%s' or no enough permissions to list.", folder);

          logger.logWarnWithTracker(runId, null, msg);
        } else {

          logger.logInfoWithTracker(
              runId,
              null,
              String.format("Found %s projects under folder %s", folderProjects.size(), folder));
        }

      } catch (Exception exception) {
        // log and continue
        logger.logFailedDispatcherEntityId(
            runId, null, folder.toString(), exception.getMessage(), exception.getClass().getName());
      }
    }
    return processProjects(
        projectIncludeList,
        projectExcludeList,
        datasetExcludeList,
        projectExcludeListPatterns,
        datasetExcludeListPatterns);
  }
}
