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

package com.google.cloud.pso.bq_snapshot_manager.helpers;


import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableOperationRequestResponse;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.TimeTravelOffsetDays;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class Utils {

  public static final Long SECONDS_IN_DAY = 86400L;
  public static final Long MILLI_SECONDS_IN_DAY = 86400000L;

  public static String getOrFail(Map<String, String> map, String key) {
    String field = map.get(key);
    if (field == null) {
      throw new IllegalArgumentException(String.format("Key '%s' is not found in Map.", key));
    } else {
      return field;
    }
  }

  public static List<String> tokenize(String input, String delimiter, boolean required) {
    List<String> output = new ArrayList<>();

    if (input.isBlank() && required) {
      throw new IllegalArgumentException(String.format("Input string '%s' is blank.", input));
    }

    if (input.isBlank() && !required) {
      return output;
    }

    StringTokenizer tokens = new StringTokenizer(input, delimiter);
    while (tokens.hasMoreTokens()) {
      output.add(tokens.nextToken().trim());
    }
    if (required && output.size() == 0) {
      throw new IllegalArgumentException(
          String.format("No tokens found in string: '%s' using delimiter '%s'", input, delimiter));
    }
    return output;
  }

  public static String getConfigFromEnv(String config, boolean required) {
    String value = System.getenv().getOrDefault(config, "");

    if (required && value.isBlank()) {
      throw new IllegalArgumentException(
          String.format("Missing environment variable '%s'", config));
    }

    return value;
  }

  public static void runServiceStartRoutines(
      LoggingHelper logger,
      TableOperationRequestResponse request,
      PersistentSet persistentSet,
      String persistentSetObjectPrefix,
      String trackingId)
      throws NonRetryableApplicationException {
    logger.logFunctionStart(request.getTrackingId(), request.getTargetTable());
    logger.logInfoWithTracker(
        request.isDryRun(),
        request.getTrackingId(),
        request.getTargetTable(),
        String.format("Request : %s", request.toString()));

    /**
     * Check if we already processed this table before by this service to avoid submitting extra API
     * requests in case we have duplicate PubSub messages. This is an extra measure to avoid
     * unnecessary cost. We do that by keeping simple flag files in GCS with the pubSubMessageId as
     * file name.
     */
    String flagFileName = String.format("%s/%s", persistentSetObjectPrefix, trackingId);
    if (persistentSet.contains(flagFileName)) {
      // log error and ACK and return
      String msg =
          String.format(
              "tracking_id '%s' has been processed before by the service. This could be a PubSub duplicate message and safe to ignore or the previous messages were not ACK to PubSub to stop retries. Please investigate further if needed.",
              trackingId);
      throw new NonRetryableApplicationException(msg);
    }
  }

  public static void runServiceEndRoutines(
      LoggingHelper logger,
      TableOperationRequestResponse request,
      PersistentSet persistentSet,
      String persistentSetObjectPrefix,
      String trackingId) {
    // Add a flag key marking that we already completed this request and no additional runs
    // are required in case PubSub is in a loop of retrying due to ACK timeout while the service
    // has
    // already processed the request
    // This is an extra measure to avoid unnecessary cost due to config issues.
    String flagFileName = String.format("%s/%s", persistentSetObjectPrefix, trackingId);
    logger.logInfoWithTracker(
        request.isDryRun(),
        request.getTrackingId(),
        request.getTargetTable(),
        String.format("Persisting processing key for tracking_id %s", trackingId));
    persistentSet.add(flagFileName);

    logger.logFunctionEnd(request.getTrackingId(), request.getTargetTable());
  }

  /**
   * return a Tuple (X, Y) where X is a TableSpec containing a table decorator with time travel
   * applied and Y is the calculated timestamp in milliseconds since epoch used for the decorator
   *
   * @param table
   * @param timeTravelOffsetDays
   * @param referencePoint
   * @return
   */
  public static Tuple<TableSpec, Long> getTableSpecWithTimeTravel(
      TableSpec table, TimeTravelOffsetDays timeTravelOffsetDays, Timestamp referencePoint) {
    Long timeTravelMs;
    Long refPointMs = timestampToUnixTimeMillis(referencePoint);

    if (timeTravelOffsetDays.equals(TimeTravelOffsetDays.DAYS_0)) {
      // always use time travel for consistency and traceability
      timeTravelMs = refPointMs;
    } else {
      // use a buffer (milliseconds) to count for the operation time. 1 MIN = 60000
      // MILLISECONDS
      Long bufferMs = timeTravelOffsetDays.equals(TimeTravelOffsetDays.DAYS_7) ? 60 * 60000L : 0L;
      // milli seconds per day * number of days
      Long timeTravelOffsetMs =
          (Utils.MILLI_SECONDS_IN_DAY * Long.parseLong(timeTravelOffsetDays.getText()));
      timeTravelMs = (refPointMs - timeTravelOffsetMs) + bufferMs;
    }

    return Tuple.of(
        new TableSpec(
            table.getProject(),
            table.getDataset(),
            String.format("%s@%s", table.getTable(), timeTravelMs)),
        timeTravelMs);
  }

  public static Long timestampToUnixTimeMillis(Timestamp ts) {
    return ts.toSqlTimestamp().getTime();
  }

  public static String trimSlashes(String str) {
    return StringUtils.removeStart(StringUtils.removeEnd(str, "/"), "/");
  }

  public static Timestamp addSeconds(Timestamp timestamp, Long secondsDelta) {
    return Timestamp.ofTimeSecondsAndNanos(
        timestamp.getSeconds() + secondsDelta, timestamp.getNanos());
  }

  /**
   * Extract elements starting with a certain prefix, compile them and return them in a new List
   *
   * @param list
   * @return List of compiled regular expression patterns
   */
  public static List<Pattern> extractAndCompilePatterns(List<String> list, String prefix) {
    List<Pattern> patterns = new ArrayList<>();
    for (String element : list) {
      if (element.toLowerCase().startsWith(prefix)) {
        String regex = element.substring(prefix.length());
        patterns.add(Pattern.compile(regex));
      }
    }
    return patterns;
  }

  public static Tuple<Boolean, String> isElementMatchLiteralOrRegexList(
      String input, List<String> list, List<Pattern> patterns) {

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
}
