package com.startup;

import com.open.qbes.api.http.Constants;
import org.kohsuke.args4j.Option;

public final class StartupOptions {

    @Option(name = "-resources", aliases = {"-r"}, required = true, usage = "The module for which QBES will be run", metaVar = "string")
    public static String resources;

    @Option(name = "-configs", aliases = {"-c"}, required = true, usage = "Full path of config directory in which properties files are placed", metaVar = "path")
    public static String configs;

    @Option(name = "-tests", aliases = {"-t"}, usage = "(Optional) Runs QBES in test mode", metaVar = "boolean")
    public static boolean testMode;

    @Option(name = "-loadOrgsFromDB", aliases = {"-ldb"}, usage = "(Optional) loads orgs from DB in test mode", metaVar = "boolean")
    public static boolean loadOrgsFromDB;

    @Option(name = "-port", aliases = {"-p"}, usage = "Port on which QBES is run. Default is 8181", metaVar = "int")
    public static int port = 8181;

    @Option(name = "-context", aliases = {"-ctx"}, usage = "Context root for the server. Default is /qbes. Note that " + Constants.API_VERSION + " will automatically be appended", metaVar = "string")
    public static String context = "/qbes";

    @Option(name = "-tests:suppress-server-logs", aliases = {"-t:sl"}, usage = "(Optional) Suppresses the server logs during system tests. Note that test logs are not suppressed. Default is true", metaVar = "string")
    public static String suppressServerLogsInTests = "TRUE";

    @Option(name = "-load-fixtures", aliases = {"-lf"}, usage = "(Optional) Loads the fixture classes to generate test data. Default is false", metaVar = "boolean")
    public static boolean loadFixtures;

    @Option(name = "-concatenate-errors", aliases = {"-cer"}, usage = "(Optional) Concatenate errors in a single line while logging (\\n) is replaced by '-errors-joiner' property value. Default is false", metaVar = "boolean")
    public static boolean concatenateErrors;

    @Option(name = "-errors-joiner", aliases = {"-ej"}, usage = "(Optional) Concatenates errors in a report using the specified pattern. Default is ... For new line (\\n), use N_L", metaVar = "string")
    public static String errorsJoiner = "...";

}
