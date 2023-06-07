package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FallbackBackupPolicy {

    private final BackupPolicy defaultPolicy;
    private final Map<String, BackupPolicy> folderOverrides;
    private final Map<String, BackupPolicy> projectOverrides;
    private final Map<String, BackupPolicy> datasetOverrides;
    private final Map<String, BackupPolicy> tableOverrides;

    public FallbackBackupPolicy(BackupPolicy defaultPolicy, Map<String, BackupPolicy> folderOverrides, Map<String, BackupPolicy> projectOverrides, Map<String, BackupPolicy> datasetOverrides, Map<String, BackupPolicy> tableOverrides) {
        this.defaultPolicy = defaultPolicy;
        this.folderOverrides = folderOverrides;
        this.projectOverrides = projectOverrides;
        this.datasetOverrides = datasetOverrides;
        this.tableOverrides = tableOverrides;
    }

    public BackupPolicy getDefaultPolicy() {
        return defaultPolicy;
    }

    public Map<String, BackupPolicy> getFolderOverrides() {
        return folderOverrides;
    }

    public Map<String, BackupPolicy> getProjectOverrides() {
        return projectOverrides;
    }

    public Map<String, BackupPolicy> getDatasetOverrides() {
        return datasetOverrides;
    }

    public Map<String, BackupPolicy> getTableOverrides() {
        return tableOverrides;
    }

    @Override
    public String toString() {
        return "FallbackBackupPolicy{" +
                "defaultPolicy=" + defaultPolicy +
                ", folderOverrides=" + folderOverrides +
                ", projectOverrides=" + projectOverrides +
                ", datasetOverrides=" + datasetOverrides +
                ", tableOverrides=" + tableOverrides +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FallbackBackupPolicy that = (FallbackBackupPolicy) o;
        return getDefaultPolicy().equals(that.getDefaultPolicy()) &&
                getFolderOverrides().equals(that.getFolderOverrides()) &&
                getProjectOverrides().equals(that.getProjectOverrides()) &&
                getDatasetOverrides().equals(that.getDatasetOverrides()) &&
                getTableOverrides().equals(that.getTableOverrides());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDefaultPolicy(), getFolderOverrides(), getProjectOverrides(), getDatasetOverrides(), getTableOverrides());
    }

    public static FallbackBackupPolicy fromJson(String jsonPolicy) throws JsonProcessingException {

        // parse json string for the "default_policy" node
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonPolicy);

        // parse the top level json nodes

        JsonNode defaultPolicyNode = root.path("default_policy");
        if (defaultPolicyNode == null){
            throw new IllegalArgumentException("'default_policy' node is missing in the provided jsonPolicy");
        }

        JsonNode folderOverridesNode = root.path("folder_overrides");
        if (folderOverridesNode == null){
            throw new IllegalArgumentException("'folder_overrides' node is missing in the provided jsonPolicy");
        }

        JsonNode projectOverridesNode = root.path("project_overrides");
        if (projectOverridesNode == null){
            throw new IllegalArgumentException("'project_overrides' node is missing in the provided jsonPolicy");
        }

        JsonNode datasetOverridesNode = root.path("dataset_overrides");
        if (datasetOverridesNode == null){
            throw new IllegalArgumentException("'dataset_overrides' node is missing in the provided jsonPolicy");
        }

        JsonNode tableOverridesNode = root.path("table_overrides");
        if (tableOverridesNode == null){
            throw new IllegalArgumentException("'table_overrides' node is missing in the provided jsonPolicy");
        }

        // parse the nodes to corresponding java classes
        BackupPolicy defaultBackupPolicy = BackupPolicy.fromJson(defaultPolicyNode.toPrettyString());

        Map<String, BackupPolicy> folderOverridesPolicies = policiesFromJsonNode(folderOverridesNode);

        Map<String, BackupPolicy> projectOverridesPolicies = policiesFromJsonNode(projectOverridesNode);

        Map<String, BackupPolicy> datasetOverridesPolicies = policiesFromJsonNode(datasetOverridesNode);

        Map<String, BackupPolicy> tableOverridesPolicies = policiesFromJsonNode(tableOverridesNode);

        return new FallbackBackupPolicy(
                defaultBackupPolicy,
                folderOverridesPolicies,
                projectOverridesPolicies,
                datasetOverridesPolicies,
                tableOverridesPolicies
        );
    }

    private static Map<String, BackupPolicy> policiesFromJsonNode(JsonNode jsonNode){

        Map<String, BackupPolicy> policyMap = new HashMap<>();

        Gson gson = new Gson();
        // each node consists of K,V pair where the K is string and V is map corresponding of a backup policy
        Type jsonNodeType = new TypeToken<Map<String, Map<String, String>>>() {}.getType();
        Map<String, Map<String, String>> jsonNodeMap = gson.fromJson(
                jsonNode.toPrettyString(),
                jsonNodeType);

        for(String key: jsonNodeMap.keySet()){
            policyMap.put(
                    key,
                    BackupPolicy.fromMap(jsonNodeMap.get(key))
            );
        }

        return policyMap;
    }
}
