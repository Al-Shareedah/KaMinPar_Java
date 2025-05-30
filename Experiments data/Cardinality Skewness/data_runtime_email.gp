set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "runtime_email.eps"

# Title and axis labels
set title "Runtime vs. Skewness Factor {/Times-Italic f}" font ",40"
set xlabel "Skewness Factor {/Times-Italic f}" font ",45"
set ylabel "Runtime (seconds)" font ",42"

# Tick and axis formatting
set tics font ",37"
unset format


# Legend
set key top left font ",32" box opaque

# Grid and margin
set grid


# Line styles
set style line 1 lw 5 pt 7 ps 4 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 4 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 4 lc rgb "#2ca02c"   # ReNUP ε=0
set style line 4 lw 5 pt 13 ps 4 lc rgb "#d62728"  # ReNUP ε=50

# Plot
plot "data_runtime_email.txt" using 1:2 title "KaMinPar"         with linespoints ls 1, \
     "data_runtime_email.txt" using 1:3 title "KaHIP"            with linespoints ls 2, \
     "data_runtime_email.txt" using 1:4 title "ReNUP, {/Symbol d}=0"       with linespoints ls 3, \
     "data_runtime_email.txt" using 1:5 title "ReNUP, {/Symbol d}=50"     with linespoints ls 4

unset output
