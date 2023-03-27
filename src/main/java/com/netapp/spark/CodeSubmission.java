package com.netapp.spark;

public record CodeSubmission(CodeSubmissionType type, String code, String className, String config, String resultFormat, String resultsPath) {
}
