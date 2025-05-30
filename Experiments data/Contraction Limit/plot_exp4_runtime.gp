# Set terminal and output
set terminal postscript eps enhanced color font "Arial,26"
set output 'runtime.eps'

# Set title and labels
set title "Impact of Graph Size on Runtime" font ",26"

set ylabel "Runtime (seconds)" font ",26"


# Set xtics
set xtics font ",23" rotate by -45
set xtics nomirror
set grid xtics ytics

# Increase margin for rotated labels
set lmargin at screen 0.22
set rmargin at screen 0.91
set bmargin at screen 0.30
set tmargin at screen 0.89

# Read data
set datafile separator whitespace

# Define line styles
unset style data
set style line 1 lt 1 lw 3 pt 7 ps 2.0 lc rgb "#1f77b4" # Blue
set style line 2 lt 1 lw 3 pt 5 ps 2.0 lc rgb "#ff7f0e" # Orange
set style line 3 lt 1 lw 3 pt 9 ps 2.0 lc rgb "#2ca02c" # Green
set style line 4 lt 1 lw 3 pt 13 ps 2.0 lc rgb "#d62728" # Red

# Set legend (key) position to top right inside the plot
set key top left inside font ",24"

# Plot as line plot with points
plot 'Exp4_runtime.txt' using 0:2:xtic(1) with linespoints linestyle 1 title "KaMinPar", \
     '' using 0:3 with linespoints linestyle 2 title "KaHIP", \
     '' using 0:4 with linespoints linestyle 3 title "ReNUP ({/Symbol d} = 0.0)", \
     '' using 0:5 with linespoints linestyle 4 title "ReNUP (Relaxed {/Symbol d})"
