package org.alshar.common;


import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class Logger {
    public static final TextFormatter DEFAULT_TEXT = new DefaultTextFormatter();
    public static final Colorized RED = new Colorized(Colorized.Color.RED);
    public static final Colorized GREEN = new Colorized(Colorized.Color.GREEN);
    public static final Colorized MAGENTA = new Colorized(Colorized.Color.MAGENTA);
    public static final Colorized ORANGE = new Colorized(Colorized.Color.ORANGE);
    public static final Colorized CYAN = new Colorized(Colorized.Color.CYAN);
    public static final Colorized RESET = new Colorized(Colorized.Color.RESET);
    public static final CompactContainerFormatter DEFAULT_CONTAINER = new CompactContainerFormatter(", ");
    public static final CompactContainerFormatter COMPACT = new CompactContainerFormatter(",");
    public static final Table TABLE = new Table(0);

    // Add the color constants
    public static final String RED_TEXT = "\u001b[31m";
    public static final String GREEN_TEXT = "\u001b[32m";
    public static final String ORANGE_TEXT = "\u001b[33m";
    public static final String CYAN_TEXT = "\u001b[36m";
    public static final String RESET_TEXT = "\u001b[0m";

    private static AtomicBoolean quietMode = new AtomicBoolean(false);

    private PrintStream out;
    private StringBuilder buffer;
    private String append;
    private boolean flushed;
    private TextFormatter textFormatter;
    private ContainerFormatter containerFormatter;

    public Logger() {
        this(System.out);
    }

    public Logger(PrintStream out) {
        this(out, "\n");
    }

    public Logger(PrintStream out, String append) {
        this.out = out;
        this.append = append;
        this.buffer = new StringBuilder();
        this.flushed = false;
        this.textFormatter = DEFAULT_TEXT;
        this.containerFormatter = DEFAULT_CONTAINER;
    }

    public void flush() {
        if (quietMode.get()) {
            return;
        }

        if (!flushed) {
            synchronized (Logger.class) {
                out.print(buffer.toString() + append);
                out.flush();
            }
        }

        flushed = true;
    }

    public static void setQuietMode(boolean quiet) {
        quietMode.set(quiet);
    }

    public static boolean isQuiet() {
        return quietMode.get();
    }

    public Logger logText(Object arg) {
        textFormatter.print(arg.toString(), buffer);
        return this;
    }

    public Logger logContainer(Iterable<?> container) {
        List<String> strList = new ArrayList<>();
        for (Object element : container) {
            strList.add(element.toString());
        }
        containerFormatter.print(strList, buffer);
        return this;
    }

    public Logger logFormatter(TextFormatter formatter) {
        this.textFormatter = formatter;
        return this;
    }

    public Logger logFormatter(ContainerFormatter formatter) {
        this.containerFormatter = formatter;
        return this;
    }

    public static void log(Object message) {
        Logger logger = new Logger();
        if (message instanceof Iterable) {
            logger.logContainer((Iterable<?>) message);
        } else if (message instanceof TextFormatter) {
            logger.logFormatter((TextFormatter) message);
        } else if (message instanceof ContainerFormatter) {
            logger.logFormatter((ContainerFormatter) message);
        } else {
            logger.logText(message);
        }
        logger.flush();
    }

    public static class DisposableLogger {
        private Logger logger;
        private boolean abortOnDestruction;

        public DisposableLogger(PrintStream out, boolean abortOnDestruction) {
            this.logger = new Logger(out);
            this.abortOnDestruction = abortOnDestruction;
        }

        public DisposableLogger log(Object arg) {
            if (arg instanceof Iterable) {
                logger.logContainer((Iterable<?>) arg);
            } else if (arg instanceof TextFormatter) {
                logger.logFormatter((TextFormatter) arg);
            } else if (arg instanceof ContainerFormatter) {
                logger.logFormatter((ContainerFormatter) arg);
            } else {
                logger.logText(arg);
            }
            return this;
        }

        @Override
        protected void finalize() throws Throwable {
            logger.logText(RESET);
            logger.flush();
            if (abortOnDestruction) {
                System.exit(1);
            }
        }
    }

    // Text Formatter Interfaces and Implementations
    public interface TextFormatter {
        void print(String text, StringBuilder out);
    }

    public static class DefaultTextFormatter implements TextFormatter {
        @Override
        public void print(String text, StringBuilder out) {
            out.append(text);
        }
    }

    public static class Colorized implements TextFormatter {
        public enum Color {
            RED("\u001b[31m"), GREEN("\u001b[32m"), MAGENTA("\u001b[35m"), ORANGE("\u001b[33m"),
            CYAN("\u001b[36m"), RESET("\u001b[0m");

            private final String code;

            Color(String code) {
                this.code = code;
            }

            public String getCode() {
                return code;
            }
        }

        private Color color;

        public Colorized(Color color) {
            this.color = color;
        }

        @Override
        public void print(String text, StringBuilder out) {
            out.append(color.getCode()).append(text).append(Color.RESET.getCode());
        }
    }

    // Container Formatter Interfaces and Implementations
    public interface ContainerFormatter {
        void print(List<String> container, StringBuilder out);
    }

    public static class CompactContainerFormatter implements ContainerFormatter {
        private String sep;

        public CompactContainerFormatter(String sep) {
            this.sep = sep;
        }

        @Override
        public void print(List<String> container, StringBuilder out) {
            for (int i = 0; i < container.size(); i++) {
                if (i > 0) {
                    out.append(sep);
                }
                out.append(container.get(i));
            }
        }
    }

    public static class Table implements ContainerFormatter {
        private int width;

        public Table(int width) {
            this.width = width;
        }

        @Override
        public void print(List<String> container, StringBuilder out) {
            int maxWidth = container.stream().mapToInt(String::length).max().orElse(0);
            int columnWidth = width == 0 ? (int) Math.sqrt(container.size()) : width;

            String rowDelimiter = "+" + "-".repeat(maxWidth + 2);
            for (int i = 1; i < columnWidth; i++) {
                rowDelimiter += "+" + "-".repeat(maxWidth + 2);
            }
            rowDelimiter += "+\n";

            out.append(rowDelimiter);
            for (int i = 0; i < container.size(); i++) {
                if (i > 0 && i % columnWidth == 0) {
                    out.append("|\n").append(rowDelimiter);
                }
                out.append("| ").append(container.get(i)).append(" ".repeat(maxWidth - container.get(i).length())).append(" ");
            }
            out.append("|\n").append(rowDelimiter);
        }
    }
}