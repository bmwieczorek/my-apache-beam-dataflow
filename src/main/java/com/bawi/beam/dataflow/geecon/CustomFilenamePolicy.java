package com.bawi.beam.dataflow.geecon;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.currentTimeMillis;

public class CustomFilenamePolicy extends FileBasedSink.FilenamePolicy {
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomFilenamePolicy.class);

    private final ValueProvider<String> outputParentPath;
    private final String filenameSuffix;

    CustomFilenamePolicy(ValueProvider<String> outputParentPath, String filenameSuffix) {
        this.outputParentPath = outputParentPath;
        this.filenameSuffix = filenameSuffix;
    }

    @Override
    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
        long windowStartMillis = ((IntervalWindow) window).start().getMillis();
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
        String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
        return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
        long windowStartMillis = currentTimeMillis();
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
        String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
        return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    private String getFilePath(ResourceId resource, long timestampMillis, int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
        String parentDirectoryPath = resource.isDirectory() ? resource.toString() : resource.getFilename();
        String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
        String suffix = suggestedFilenameSuffix == null || suggestedFilenameSuffix.isEmpty() ? filenameSuffix : suggestedFilenameSuffix;
        String filename = String.format("%s--%s-of-%s%s", FORMATTER.print(timestampMillis), shardNumber, numShards, suffix);
//            String randomFilePrefix = DigestUtils.md5Hex(UUID.randomUUID() + filename + timestampMillis).substring(0, 6);
        String randomFilePrefix = DigestUtils.md5Hex(timestampMillis + "" + shardNumber).substring(0, 6);
        String outputFilePath = String.format("%s%s--%s", parentDirectoryPath, randomFilePrefix, filename);
//            String outputFilePath = String.format("%s%s", parentDirectoryPath, MySeqGenToGCSWriteJob.class.getSimpleName().toLowerCase() + "/" + filename);
        LOGGER.info("Writing file to {}", outputFilePath);
        return outputFilePath;
    }
}