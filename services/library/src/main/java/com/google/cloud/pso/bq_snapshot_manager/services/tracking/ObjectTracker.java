package com.google.cloud.pso.bq_snapshot_manager.services.tracking;

import java.io.IOException;
import java.util.List;

public interface ObjectTracker {

    void trackObjects(List<Object> objects, String runId) throws IOException;
}
