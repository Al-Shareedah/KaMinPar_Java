set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "runtime_4elt.eps"

# Updated title and labels for runtime
set title "Impact of Number of Partitions on Runtime" font ",40"
set xlabel "Number of Partitions (k)" font ",39"
set ylabel "Runtime (seconds)" font ",39"

# Larger tick labels
set tics font ",37"

# Set Y-axis range
set yrange [0:5.6]

# No percent format for runtime
unset format

# Legend
set key top left font ",32" box opaque

# Grid
set grid

# Line styles
set style line 1 lw 5 pt 7 ps 4 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 4 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 4 lc rgb "#2ca02c"   # ReNUP {/Symbol e}=0.0
set style line 4 lw 5 pt 13 ps 4 lc rgb "#d62728"  # ReNUP {/Symbol e}=780

# Margin
set lmargin 14

# Plot new runtime data
plot "data_4elt_runtime.txt" using 1:2 title "KaMinPar"             with linespoints ls 1, \
     "data_4elt_runtime.txt" using 1:3 title "KaHIP"                with linespoints ls 2, \
     "data_4elt_runtime.txt" using 1:4 title "ReNUP, {/Symbol d}=0.0"     with linespoints ls 3, \
     "data_4elt_runtime.txt" using 1:5 title "ReNUP, {/Symbol d}=780"     with linespoints ls 4

unset output
