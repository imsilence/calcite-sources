package com.silence.calcite.sources.v3;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class CSVSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        log.debug("csv schema factory, name: {}, operand: {}", name, operand);
        String directory = ((String) operand.getOrDefault("directory", "datas"));
        String flavor = ((String) operand.getOrDefault("flavor", ""));
        File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
        File directoryFile = new File(directory);
        if (Objects.nonNull(base) && !directoryFile.isAbsolute()) {
            directoryFile = new File(base, directory);
        }
        return new CSVSchema(directoryFile, Flavor.get(flavor));
    }
}
