package org.alshar.common;

public class LoggerMacros {
    public static final boolean kDebug = true;  // Set this based on your need

    public static Logger LOG = new Logger();
    public static Logger LLOG = new Logger(System.out, "");

    public static Logger LOG_ERROR = new Logger(System.out, "\n").logText("[Error] ").logText(Logger.RED_TEXT);
    public static Logger LOG_LERROR = new Logger(System.out, "").logText("").logText(Logger.RED_TEXT);
    public static Logger LOG_SUCCESS = new Logger(System.out, "\n").logText("[Success] ").logText(Logger.GREEN_TEXT);
    public static Logger LOG_LSUCCESS = new Logger(System.out, "").logText("").logText(Logger.GREEN_TEXT);
    public static Logger LOG_WARNING = new Logger(System.out, "\n").logText("[Warning] ").logText(Logger.ORANGE_TEXT);
    public static Logger LOG_LWARNING = new Logger(System.out, "").logText("").logText(Logger.ORANGE_TEXT);
    public static Logger FATAL_ERROR = new Logger(System.out, "\n").logText("[Fatal] ").logText(Logger.RED_TEXT);
    public static Logger FATAL_PERROR = new Logger(System.out, "\n").logText("[Fatal] ").logText(Logger.RED_TEXT).logText(System.err.toString());

    public static void SET_DEBUG(boolean value) {
        // Set debug value here
    }

    public static void DBG(String message) {
        if (kDebug) {
            new Logger().logText(message).flush();
        }
    }

    public static void IFDBG(Runnable expression) {
        if (kDebug) {
            expression.run();
        }
    }

    public static void LOG_STATS(String message) {
        if (kDebug) {
            new Logger().logText(message).logText(Logger.CYAN_TEXT).flush();
        }
    }

    // Other macros as per your requirement
}