package org.alshar.kaminpar_shm.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class MappedFileToker {
    private MappedByteBuffer buffer;
    private int position;
    private int length;

    public MappedFileToker(String filename) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            FileChannel channel = file.getChannel();
            this.length = (int) channel.size();
            this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);
        }
        this.position = 0;
    }

    public void skipSpaces() {
        while (validPosition() && current() == ' ') {
            advance();
        }
    }

    public void skipLine() {
        while (validPosition() && current() != '\n') {
            advance();
        }
        if (validPosition()) {
            advance();
        }
    }

    public long scanUInt() {
        expectUIntStart();

        long number = 0;
        while (validPosition() && Character.isDigit(current())) {
            int digit = current() - '0';
            number = number * 10 + digit;
            advance();
        }
        skipSpaces();
        return number;
    }

    public void consumeChar(char ch) {
        if (!validPosition() || current() != ch) {
            throw new IllegalStateException("Unexpected character: " + current() + ", expected: " + ch);
        }
        advance();
    }

    public boolean validPosition() {
        return position < length;
    }

    public char current() {
        return (char) buffer.get(position);
    }

    public void advance() {
        position++;
    }

    public int position() {
        return position;
    }

    public int length() {
        return length;
    }

    private void expectUIntStart() {
        if (!validPosition() || !Character.isDigit(current())) {
            throw new IllegalStateException("Expected start of unsigned integer");
        }
    }
}
