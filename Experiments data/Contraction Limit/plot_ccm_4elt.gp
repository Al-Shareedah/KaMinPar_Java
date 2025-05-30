# Set terminal to EPS with larger font
set terminal postscript eps enhanced color font 'Helvetica,36'

# Output file
set output 'ccm_4elt.eps'

# Set title with epsilon symbol
set title "Impact on CCM as {/Symbol d} varies (4elt graph)" font ",30"

# Set mathematically formatted axis labels with correct minus sign
set xlabel "{/Symbol d} values (Upper Bound = min(Partition size) {/Symbol \55} 1)" font ",28"
set ylabel "CCM (%)" font ",28"

# Set grid
set grid

# Set Y range (top slightly above max value 45.85%)
set yrange [* : 45]
set xrange [* : 2600]

# Format y-axis to show percentage
set format y "%.0f%%"

# Set style for lines and points
set style line 1 lt 1 lw 3 pt 7 ps 2.0 lc rgb "#1f77b4"   # ReNUP
set style line 2 lt 1 lw 3 pt 5 ps 2.0 lc rgb "#ff7f0e"   # KaMinPar 
set style line 3 lt 1 lw 3 pt 9 ps 2.0 lc rgb "#2ca02c"   # KaHIP   

# Set legend (key) to the left
set key left font ",32"

# Increase tick font sizes
set xtics font ",26"
set ytics font ",26"



# Plot all three series from the same file:
plot 'data_ccm_4elt.txt' using 1:2 with linespoints   linestyle 1 title 'ReNUP', \
'data_ccm_4elt.txt' using 1:3 with linespoints   linestyle 2 title 'KaMinPar', \
'data_ccm_4elt.txt' using 1:4 with linespoints   linestyle 3 title 'KaHIP'
