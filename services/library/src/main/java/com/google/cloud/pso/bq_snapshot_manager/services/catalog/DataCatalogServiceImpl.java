/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.pso.bq_snapshot_manager.services.catalog;


import com.google.cloud.Timestamp;
import com.google.cloud.datacatalog.v1.*;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataCatalogServiceImpl implements DataCatalogService {

    private final DataCatalogClient dataCatalogClient;

    public DataCatalogServiceImpl() throws IOException {
        dataCatalogClient = DataCatalogClient.create();
    }

    public void shutdown(){
        dataCatalogClient.shutdown();
    }

    public Tag createOrUpdateBackupPolicyTag(TableSpec tableSpec, BackupPolicy backupPolicy, String backupPolicyTagTemplateId){

        // API Call
        String parent = getBigQueryEntryName(tableSpec);

        // API CALL
        DataCatalogClient.ListTagsPagedResponse response = dataCatalogClient.listTags(parent);

        List<Tag> allTags = new ArrayList<>();
        for (DataCatalogClient.ListTagsPage l: response.iteratePages()){
            allTags.addAll(l.getResponse().getTagsList());
        }

        Tag tag = findTag(
                allTags,
                backupPolicyTagTemplateId
        );

        if(tag == null){
            // create a new tag
            return dataCatalogClient.createTag(parent, backupPolicy.toDataCatalogTag(backupPolicyTagTemplateId, null));
        }else{
            // update existing tag referencing the existing tag.name
            return  dataCatalogClient.updateTag(
                    backupPolicy.toDataCatalogTag(
                            backupPolicyTagTemplateId,
                            tag.getName()
                    )
            );
        }
    }

    public Tag findTag(List<Tag> tags, String tagTemplateName){

        List<Tag> foundTags = tags.stream().filter(t -> t.getTemplate().equals(tagTemplateName))
                .collect(Collectors.toList());

        // if more than one tag is found use the first one
        return foundTags.size() >= 1? foundTags.get(0): null;
    }


    /**
     * Return the attached backup policy tag template or null if no template is attached
     *
     * @param tableSpec
     * @param backupPolicyTagTemplateId
     * @return
     * @throws IllegalArgumentException
     */
    public @Nullable BackupPolicy getBackupPolicyTag(TableSpec tableSpec, String backupPolicyTagTemplateId) throws IllegalArgumentException {

        Map<String, TagField> tagTemplate = getTagFieldsMap(tableSpec, backupPolicyTagTemplateId);

        if(tagTemplate == null){
            // no backup tag template is attached to this table
            return null;
        }else{
            return BackupPolicy.fromMap(convertTagFieldMapToStrMap(tagTemplate));
        }
    }

    public Tag getTag(TableSpec tableSpec, String templateId){
        // API Call
        String parent = getBigQueryEntryName(tableSpec);
        // API CALL
        DataCatalogClient.ListTagsPagedResponse response = dataCatalogClient.listTags(parent);

        // TODO: handle multiple pages
        List<Tag> tags = response.getPage().getResponse().getTagsList();

        for (Tag tagTemplate: tags){
            if (tagTemplate.getTemplate().equals(templateId)){
                return tagTemplate;
            }
        }
        return null;
    }

    public Map<String, TagField> getTagFieldsMap(TableSpec tableSpec, String templateId) {

        Tag tag = getTag(tableSpec, templateId);
        return tag == null? null: tag.getFieldsMap();
    }

    public String getBigQueryEntryName(TableSpec tableSpec){
        LookupEntryRequest lookupEntryRequest =
                LookupEntryRequest.newBuilder()
                        .setLinkedResource(tableSpec.toDataCatalogLinkedResource()).build();

        // API Call
        return dataCatalogClient.lookupEntry(lookupEntryRequest).getName();
    }

    public static Map<String, String> convertTagFieldMapToStrMap(Map<String, TagField> tagFieldMap){

        Map<String, String> strMap = new HashMap<>(tagFieldMap.size());
        for(Map.Entry<String, TagField> entry: tagFieldMap.entrySet()){
            String strValue = "";
            if(entry.getValue().hasBoolValue()){
                strValue = String.valueOf(entry.getValue().getBoolValue());
            }
            if(entry.getValue().hasStringValue()){
                strValue = entry.getValue().getStringValue();
            }
            if(entry.getValue().hasDoubleValue()){
                strValue = String.valueOf(entry.getValue().getDoubleValue());
            }
            if(entry.getValue().hasEnumValue()){
                strValue = entry.getValue().getEnumValue().getDisplayName();
            }
            if(entry.getValue().hasTimestampValue()){
                strValue = Timestamp.ofTimeSecondsAndNanos(
                        entry.getValue().getTimestampValue().getSeconds(),
                        entry.getValue().getTimestampValue().getNanos()
                ).toString();
            }
            if(entry.getValue().hasRichtextValue()){
                strValue = entry.getValue().getRichtextValue();
            }
            strMap.put(entry.getKey(), strValue);
        }
        return strMap;
    }


}
