package com.netapp.spark;

public class CodeSubmission {
    CodeSubmissionType type;
    String code;
    String className;
    String config;
    String resultFormat;
    String resultsPath;

    public CodeSubmissionType getType() {
        return type;
    }

    public void setType(CodeSubmissionType type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getResultFormat() {
        return resultFormat;
    }

    public void setResultFormat(String resultFormat) {
        this.resultFormat = resultFormat;
    }

    public String getResultsPath() {
        return resultsPath;
    }

    public void setResultsPath(String resultsPath) {
        this.resultsPath = resultsPath;
    }

    public CodeSubmission() {}

    public CodeSubmission(CodeSubmissionType type, String code, String className, String config, String resultFormat, String resultsPath) {
        this.type = type;
        this.code = code;
        this.className = className;
        this.config = config;
        this.resultFormat = resultFormat;
        this.resultsPath = resultsPath;
    }

    public CodeSubmissionType type() {
        return type;
    }

    public String code() {
        return code;
    }

    public String className() {
        return className;
    }

    public String config() {
        return config;
    }

    public String resultFormat() {
        return resultFormat;
    }

    public String resultsPath() {
        return resultsPath;
    }

    public void type(CodeSubmissionType type) {
        this.type = type;
    }

    public void code(String code) {
        this.code = code;
    }

    public void className(String className) {
        this.className = className;
    }

    public void config(String config) {
        this.config = config;
    }

    public void resultFormat(String resultFormat) {
        this.resultFormat = resultFormat;
    }

    public void resultsPath(String resultsPath) {
        this.resultsPath = resultsPath;
    }
}