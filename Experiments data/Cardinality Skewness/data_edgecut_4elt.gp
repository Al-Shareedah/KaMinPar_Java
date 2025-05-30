set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "edgecut_4elt.eps"

# Title and axis labels
set title "Edge Cut Improvement vs. Skewness Factor {/Times-Italic f}" font ",43"
set xlabel "Skewness Factor {/Times-Italic f}" font ",42"
set ylabel "Improvement over KaHIP (%)" font ",39"

# Tick and axis formatting
set format y "%g%%"
set tics font ",37"
set yrange [-40:20]

# Legend
set key top right font ",32" box opaque

# Grid and margin
set grid


# Line styles
set style line 1 lw 5 pt 7 ps 4 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 4 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 4 lc rgb "#2ca02c"   # ReNUP ε=0
set style line 4 lw 5 pt 13 ps 4 lc rgb "#d62728"  # ReNUP ε=780

# Plot
plot "data_edgecut_4elt.txt" using 1:2 title "KaMinPar"         with linespoints ls 1, \
     "data_edgecut_4elt.txt" using 1:3 title "KaHIP (Baseline)"            with linespoints ls 2, \
     "data_edgecut_4elt.txt" using 1:4 title "ReNUP, {/Symbol d}=0"       with linespoints ls 3, \
     "data_edgecut_4elt.txt" using 1:5 title "ReNUP, {/Symbol d}=780"     with linespoints ls 4

unset output
