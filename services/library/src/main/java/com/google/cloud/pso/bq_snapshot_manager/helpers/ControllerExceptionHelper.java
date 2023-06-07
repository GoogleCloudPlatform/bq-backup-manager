/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.bq_snapshot_manager.helpers;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.BaseServiceException;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.pso.bq_snapshot_manager.entities.RetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.common.collect.Sets;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.annotation.Nullable;
import java.util.Set;

public class ControllerExceptionHelper {

    // add more Retryable Exceptions Here
    private static final Set<Class> RETRYABLE_EXCEPTIONS = Sets.newHashSet(
            RetryableApplicationException.class,
            ResourceExhaustedException.class,
            javax.net.ssl.SSLException.class,
            java.net.SocketException.class
    );

    public static ThrowableInfo isRetryableException(Throwable throwable){

        Class exceptionClass = throwable.getClass();

        // 1. Check if it's any type of rate limit exception. If so, retry.

        // Thrown by DLP
        if(StatusRuntimeException.class.isAssignableFrom(exceptionClass)){
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) throwable;
            if (statusRuntimeException.getStatus() != null && statusRuntimeException.getStatus().equals(Status.RESOURCE_EXHAUSTED)){
                return new ThrowableInfo(throwable,true, "Retryable: 'status' = RESOURCE_EXHAUSTED assignable from StatusRuntimeException" );
            }
        }

        // Thrown by BigQuery
        if(BigQueryException.class.isAssignableFrom(exceptionClass)){
            BigQueryException bigQueryException = (BigQueryException) throwable;
            // when hitting the concurrent interactive queries
            // bigQueryException.getReason() is sometimes null.
            if (bigQueryException.getReason() != null && bigQueryException.getReason().equals("jobRateLimitExceeded")){
                return new ThrowableInfo(throwable,true, "Retryable: 'reason' = jobRateLimitExceeded assignable from BigQueryException" );
            }
            // handling "Exceeded rate limits: too many api requests per user per method for this user_method"
            if (bigQueryException.getReason() != null && bigQueryException.getReason().equals("rateLimitExceeded")){
                return new ThrowableInfo(throwable,true, "Retryable: 'reason' = rateLimitExceeded assignable from BigQueryException" );
            }
        }

        // BaseServiceException Thrown by BigQuery
        if(BaseServiceException.class.isAssignableFrom(exceptionClass)){
            BaseServiceException baseServiceException = (BaseServiceException) throwable;
            if (baseServiceException.getCode() == 429){
                return new ThrowableInfo(throwable,true, "Retryable: 'code' = 429 assignable from BaseServiceException" );
            }
        }

        // 2. check if the exception inherits (at any level) from any Retryable Exception that we explicitly define
        for(Class retryableExClass: RETRYABLE_EXCEPTIONS){
            if (retryableExClass.isAssignableFrom(exceptionClass)){
                return new ThrowableInfo(throwable,true, String.format("Retryable: isAssignableFrom %s ", retryableExClass.getName()) );
            }
        }

        // 3. check if the exception is caused by a Retryable Exception
        for(Class retryableExClass: RETRYABLE_EXCEPTIONS){
            if (retryableExClass.isAssignableFrom(exceptionClass)){
                return new ThrowableInfo(throwable,true, String.format("Retryable: isAssignableFrom %s ", retryableExClass.getName()) );
            }
        }

        // 4. Check if it's any type of isRetryable. If so, retry.

        // Thrown by BigQuery:
        // Base class for all gcp service exceptions.
        // Check if it's a retryable BaseServiceException
        if (BaseServiceException.class.isAssignableFrom(exceptionClass)){
            BaseServiceException baseServiceException = (BaseServiceException) throwable;
            return new ThrowableInfo(throwable,baseServiceException.isRetryable(), String.format("Check: isAssignableFrom BaseServiceException"));
        }

        // Check if it's Retryable ApiException
        if (ApiException.class.isAssignableFrom(exceptionClass)){
            ApiException apiEx = (ApiException) throwable;
            return new ThrowableInfo(throwable,apiEx.isRetryable(), String.format("Check: isAssignableFrom BaseServiceException"));
        }

        // if not, log and ACK so that it's not retried
        return new ThrowableInfo(throwable,false, String.format(""));

    }

    // Checks if the given throwable or recursively any of it's causes are Retryable
    private static ThrowableInfo causedByRetryableException(Throwable throwable){

        // check if the given throwable is Retryable
        ThrowableInfo throwableInfo = isRetryableException(throwable);

        // if so, stop here (recursion base case)
        if (throwableInfo.isRetryable())
            return throwableInfo;
        else{
            // if it has a cause, check if that cause is retryable

            // if it has no cause. Stop here (recursion base case)
            if (throwable.getCause() == null){
                return new ThrowableInfo(throwable, false, "");
            }else{
                // if it has a cause, check if it's retryable (recursion)
                return causedByRetryableException(throwable.getCause());
            }
        }
    }

    /**
     *
     * @param ex
     * @param logger
     * @param trackingId
     * @return Tuple<ResponseEntity, Boolean> where the Boolean value indicates if the exception is retyrable or not
     */
    public static Tuple<ResponseEntity, Boolean> handleException(Exception ex,
                                                                 LoggingHelper logger,
                                                                 String trackingId,
                                                                 @Nullable TableSpec tableSpec){

        ThrowableInfo exInfo = causedByRetryableException(ex);

        if(exInfo.isRetryable()){
            logger.logRetryableExceptions(trackingId, tableSpec, ex, exInfo.getNotes());
            return Tuple.of(
                    new ResponseEntity(ex.getMessage(), HttpStatus.TOO_MANY_REQUESTS),
                    true);

        }else{

            // if it's not retryable, log and ACK so that it's not retried
            ex.printStackTrace();
            logger.logNonRetryableExceptions(trackingId, tableSpec, ex);
            return Tuple.of(
                    new ResponseEntity(ex.getMessage(), HttpStatus.OK),
                    false);
        }
    }

}
