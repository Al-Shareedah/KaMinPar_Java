set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "runtime_pgp.eps"

# Updated title and labels for runtime
set title "Impact of {/Times-Italic k} on Runtime" font ",45"
set xlabel "Number of Partitions ({/Times-Italic k})" font ",39"
set ylabel "Runtime (seconds)" font ",39"

# Larger tick labels
set tics font ",39"

# Set Y-axis range
set yrange [0.5:4.5]


# No percent format for runtime
unset format

# Legend
set key top left font ",39" box opaque

# Grid
set grid

# Line styles
set style line 1 lw 5 pt 7 ps 5 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 5 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 5 lc rgb "#2ca02c"   # ReNUP {/Symbol e}=0.0
set style line 4 lw 5 pt 13 ps 5 lc rgb "#d62728"  # ReNUP {/Symbol e}=534

# Margin


# Plot new runtime data
plot "data_pgp_runtime.txt" using 1:2 title "KaMinPar"             with linespoints ls 1, \
     "data_pgp_runtime.txt" using 1:3 title "KaHIP"                with linespoints ls 2, \
     "data_pgp_runtime.txt" using 1:4 title "ReNUP, {/Symbol d}=0.0"     with linespoints ls 3, \
     "data_pgp_runtime.txt" using 1:5 title "ReNUP, {/Symbol d}=534"     with linespoints ls 4

unset output
