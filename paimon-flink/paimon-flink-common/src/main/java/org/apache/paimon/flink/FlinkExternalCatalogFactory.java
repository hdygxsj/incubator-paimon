package org.apache.paimon.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;
import static org.apache.paimon.options.CatalogOptions.METASTORE;

public class FlinkExternalCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "paimon-external";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
    @Override
    public FlinkExternalCatalog createCatalog(Context context) {
        return createCatalog(
                context.getClassLoader(), context.getOptions(), context.getName());
    }

    @VisibleForTesting
    public static FlinkExternalCatalog createCatalog(
            ClassLoader cl, Map<String, String> optionMap, String name) {
        Options options = Options.fromMap(optionMap);
        FlinkFileIOLoader fallbackIOLoader = new FlinkFileIOLoader();
        CatalogContext context = CatalogContext.create(options, fallbackIOLoader);
        String metastore = context.options().get(METASTORE);
        org.apache.paimon.catalog.CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(cl, org.apache.paimon.catalog.CatalogFactory.class, metastore);
        String warehouse = org.apache.paimon.catalog.CatalogFactory.warehouse(context).toUri().toString();
        Path warehousePath = new Path(warehouse);
        FileIO fileIO = org.apache.paimon.catalog.CatalogFactory.createFileIO(context, warehousePath, warehouse);
        FlinkCatalog paimon =
                new FlinkCatalog(
                        catalogFactory.create(fileIO, warehousePath, context),
                        name,
                        options.get(DEFAULT_DATABASE),
                        cl,
                        options);

        return new FlinkExternalCatalog(paimon,fileIO);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return CatalogFactory.super.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return CatalogFactory.super.optionalOptions();
    }
}
