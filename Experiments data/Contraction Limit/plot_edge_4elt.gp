# Set terminal to EPS with larger font
set terminal postscript eps enhanced color font 'Helvetica,28'

# Output file
set output 'edgecut_4elt.eps'

# Title with delta (d) symbol
set title "Impact on Edge cut as {/Symbol d} varies (4elt graph)" font ",28"

# Axis labels with correct minus sign
set xlabel "{/Symbol d} values (Upper Bound = min(Partition size) {/Symbol \\55} 1)" font ",28"
set ylabel "Edge Cut Values (lower is better)"                 font ",28"

# Grid
set grid

set xrange [0:2600]


# Line-styles
set style line 1 lt 1 lw 3 pt 7 ps 2.0 lc rgb "#1f77b4"   # ReNUP
set style line 2 lt 1 lw 3 pt 5 ps 2.0 lc rgb "#ff7f0e"   # KaMinPar 
set style line 3 lt 1 lw 3 pt 9 ps 2.0 lc rgb "#2ca02c"   # KaHIP   

# Legend on left
set key left font ",32"

# Tick fonts
set xtics font ",26"
set ytics font ",26"

# Plot all three series from the same file:
plot 'data_edgecut_4elt.txt' using 1:2 with linespoints   linestyle 1 title 'ReNUP', \
'data_edgecut_4elt.txt' using 1:3 with linespoints   linestyle 2 title 'KaMinPar', \
'data_edgecut_4elt.txt' using 1:4 with linespoints   linestyle 3 title 'KaHIP'
