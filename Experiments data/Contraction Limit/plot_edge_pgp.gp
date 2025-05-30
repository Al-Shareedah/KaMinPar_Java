# Set terminal to EPS with larger font
set terminal postscript eps enhanced color font 'Helvetica,28'

# Output file
set output 'edgecut_pgp.eps'

# Set title with epsilon symbol
set title "Impact on Edge cut as {/Symbol d} varies (pgp graph)" font ",28"

# Set mathematically formatted axis labels with correct minus sign
set xlabel "{/Symbol d} values (Upper Bound = min(Partition size) {/Symbol \55} 1)" font ",28"
set ylabel "Edge Cut Values (lower is better)" font ",28"

# Set grid
set grid




# Set style for lines and points
set style line 1 lt 1 lw 3 pt 7 ps 2.0 lc rgb "#1f77b4"   # ReNUP
set style line 2 lt 1 lw 3 pt 5 ps 2.0 lc rgb "#ff7f0e"   # KaMinPar 
set style line 3 lt 1 lw 3 pt 9 ps 2.0 lc rgb "#2ca02c"   # KaHIP  

# Set legend (key) to the left
set key left font ",32"

# Increase tick font sizes
set xtics font ",24"
set ytics font ",26"






# Plot all three series from the same file:
plot 'data_edgecut_pgp.txt' using 1:2 with linespoints   linestyle 1 title 'ReNUP', \
'data_edgecut_pgp.txt' using 1:3 with linespoints   linestyle 2 title 'KaMinPar', \
'data_edgecut_pgp.txt' using 1:4 with linespoints   linestyle 3 title 'KaHIP'
