package org.alshar.common;


import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Logger {
    public enum Color {
        RED("\u001b[31m"),
        GREEN("\u001b[32m"),
        MAGENTA("\u001b[35m"),
        ORANGE("\u001b[33m"),
        CYAN("\u001b[36m"),
        RESET("\u001b[0m");

        private final String code;

        Color(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }

    public static class ContainerFormatter {
        public void print(List<String> container, PrintStream out) {
            boolean first = true;
            for (String element : container) {
                if (!first) {
                    out.print(", ");
                }
                out.print(element);
                first = false;
            }
        }
    }

    public static class TableFormatter extends ContainerFormatter {
        private final int width;

        public TableFormatter(int width) {
            this.width = width;
        }

        @Override
        public void print(List<String> container, PrintStream out) {
            int max_width = container.stream().mapToInt(String::length).max().orElse(0);
            int actualWidth = (width == 0) ? (int) Math.sqrt(container.size()) : width;

            String hDel = "+" + "-".repeat(max_width + 2).repeat(actualWidth) + "+";
            out.println(hDel);

            int column = 0;
            for (String element : container) {
                out.print("| " + element + " ".repeat(max_width + 1 - element.length()));
                if (++column == actualWidth) {
                    out.println("|");
                    out.println(hDel);
                    column = 0;
                }
            }

            if (column > 0) {
                while (column < actualWidth) {
                    out.print("| " + " ".repeat(max_width + 1));
                    column++;
                }
                out.println("|");
                out.println(hDel);
            }
        }
    }

    public static class TextFormatter {
        public void print(String text, PrintStream out) {
            out.print(text);
        }
    }

    public static class ColorizedTextFormatter extends TextFormatter {
        private final Color color;

        public ColorizedTextFormatter(Color color) {
            this.color = color;
        }

        @Override
        public void print(String text, PrintStream out) {
            out.print(color.getCode() + text + Color.RESET.getCode());
        }
    }

    public static final TextFormatter DEFAULT_TEXT = new TextFormatter();
    public static final ColorizedTextFormatter RED_TEXT = new ColorizedTextFormatter(Color.RED);
    public static final ColorizedTextFormatter GREEN_TEXT = new ColorizedTextFormatter(Color.GREEN);
    public static final ColorizedTextFormatter MAGENTA_TEXT = new ColorizedTextFormatter(Color.MAGENTA);
    public static final ColorizedTextFormatter ORANGE_TEXT = new ColorizedTextFormatter(Color.ORANGE);
    public static final ColorizedTextFormatter CYAN_TEXT = new ColorizedTextFormatter(Color.CYAN);
    public static final ColorizedTextFormatter RESET_TEXT = new ColorizedTextFormatter(Color.RESET);
    public static final ContainerFormatter DEFAULT_CONTAINER = new ContainerFormatter();
    public static final TableFormatter TABLE_FORMATTER = new TableFormatter(0);

    private static final AtomicInteger quiet = new AtomicInteger(0);

    private final StringBuilder buffer = new StringBuilder();
    private final PrintStream out;
    private final String append;
    private boolean flushed = false;

    private TextFormatter textFormatter = DEFAULT_TEXT;
    private ContainerFormatter containerFormatter = DEFAULT_CONTAINER;

    public Logger() {
        this(System.out, "\n");
    }

    public Logger(PrintStream out, String append) {
        this.out = out;
        this.append = append;
    }

    public <T> Logger log(T message) {
        if (quiet.get() == 0) {
            if (message instanceof Collection) {
                containerFormatter.print(((Collection<?>) message).stream().map(String::valueOf).collect(Collectors.toList()), out);
            } else {
                textFormatter.print(String.valueOf(message), out);
            }
        }
        return this;
    }

    public <T> Logger log(T message, TextFormatter formatter) {
        this.textFormatter = formatter;
        return log(message);
    }

    public void flush() {
        if (quiet.get() == 0 && !flushed) {
            synchronized (out) {
                out.print(buffer.toString() + append);
                out.flush();
            }
            flushed = true;
        }
    }

    public static void setQuietMode(boolean mode) {
        quiet.set(mode ? 1 : 0);
    }

    public static boolean isQuiet() {
        return quiet.get() != 0;
    }
}