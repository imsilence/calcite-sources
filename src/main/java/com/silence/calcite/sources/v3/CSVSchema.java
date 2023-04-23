package com.silence.calcite.sources.v3;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Sources;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class CSVSchema extends AbstractSchema {
    private final File directory;
    private final Flavor flavor;

    private static final String EXT = ".csv";

    private Map<String, Table> tableMap;

    public CSVSchema(File directory, Flavor flavor) {
        this.directory = directory;
        this.flavor = flavor;
    }

    @Override
    public Map<String, Table> getTableMap() {
        if (Objects.isNull(tableMap)) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        ImmutableMap.Builder<String, Table> builder = new ImmutableMap.Builder<>();
        File[] files = directory.listFiles();
        if (Objects.isNull(files)) {
            files = new File[0];
        }
        for (File file : files) {
            String fullName = file.getName();
            int pos = FilenameUtils.indexOfExtension(fullName);
            if (pos == -1) {
                continue;
            }
            String name = fullName.substring(0, pos);
            String ext = fullName.substring(pos);
            if (!EXT.equals(ext)) {
                continue;
            }
            log.info("load table, name: {}, path: {}", name, file.getAbsoluteFile());
            builder.put(name, flavor.getTable(Sources.of(file)));
        }
        return builder.build();
    }
}
