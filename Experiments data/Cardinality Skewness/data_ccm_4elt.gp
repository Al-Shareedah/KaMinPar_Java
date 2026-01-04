set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "ccm_vs_skewness_4elt.eps"

# Title and labels
set title "CCM vs. Skewness Factor {/Times-Italic f}" font ",45"
set xlabel "Skewness Factor {/Times-Italic f}" font ",42"
set ylabel "CCM (%)" font ",39"

# Percent formatting
set format y "%g%%"
set tics font ",39"
set yrange [0:100]

# Legend
set key top right font ",40" box opaque

# Grid and margins
set grid

# Line styles
set style line 1 lw 5 pt 7 ps 5 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 5 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 5 lc rgb "#2ca02c"   # ReNUP ε=0
set style line 4 lw 5 pt 13 ps 5 lc rgb "#d62728"  # ReNUP ε=780

# Plot the data
plot "data_ccm_4elt.txt" using 1:2 title "KaMinPar"         with linespoints ls 1, \
     "data_ccm_4elt.txt" using 1:3 title "KaHIP"            with linespoints ls 2, \
     "data_ccm_4elt.txt" using 1:4 title "ReNUP, {/Symbol d}=0"       with linespoints ls 3, \
     "data_ccm_4elt.txt" using 1:5 title "ReNUP, {/Symbol d}=780"     with linespoints ls 4

unset output
