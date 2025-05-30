set terminal postscript eps enhanced color font "Arial,28" size 7in,5in
set output "edgecut_email.eps"

# Title and labels
set title "Edge Cut Improvement vs. Number of Partitions" font ",32"
set xlabel "Number of Partitions (k)" font ",39"
set ylabel "Improvement over KaHIP (%)" font ",39"

# Tick labels
set tics font ",37"

# Format y-axis with percent symbol
set format y "%g%%"

# Set y-axis range for visual clarity (adjust as needed)
set yrange [-30:25]

# Legend
set key top left font ",32" box opaque

# Grid
set grid

# Line styles
set style line 1 lw 5 pt 7 ps 4 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 4 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 4 lc rgb "#2ca02c"   # Deep NUGP {/Symbol e}=0.0
set style line 4 lw 5 pt 13 ps 4 lc rgb "#d62728"  # Deep NUGP {/Symbol e}=57

# Left margin
set lmargin 14

# Plot the percentage improvement
plot "data_email_edgecut.txt" using 1:2 title "KaMinPar"             with linespoints ls 1, \
     "data_email_edgecut.txt" using 1:3 title "KaHIP (Baseline)"     with linespoints ls 2, \
     "data_email_edgecut.txt" using 1:4 title "ReNUP, {/Symbol d}=0.0"     with linespoints ls 3, \
     "data_email_edgecut.txt" using 1:5 title "ReNUP, {/Symbol d}=57"     with linespoints ls 4

unset output
