set terminal postscript eps enhanced color font "Arial,36" size 7in,5in
set output "ccm_4elt.eps"

# Updated plot title and axis labels
set title "Impact of {/Times-Italic k} on CCM" font ",45"
set xlabel "Number of Partitions ({/Times-Italic k})" font ",39"
set ylabel "CCM (%)" font ",39"

# Large tick labels
set tics font ",39"

# Format y-axis to show percent signs
set format y "%g%%"

# Legend: top-left inside with font and box
set key top left font ",39" box opaque

# Grid lines
set grid

# Line styles: bold, large markers
set style line 1 lw 5 pt 7 ps 5 lc rgb "#1f77b4"   # KaMinPar
set style line 2 lw 5 pt 9 ps 5 lc rgb "#ff7f0e"   # KaHIP
set style line 3 lw 5 pt 5 ps 5 lc rgb "#2ca02c"   # Deep NUGP {/Symbol e}=0.0
set style line 4 lw 5 pt 13 ps 5 lc rgb "#d62728"  # Deep NUGP {/Symbol e}=57

# Add left margin for long ticks/labels


# Plot from the updated file
plot "data_4elt_ccm.txt" using 1:2 title "KaMinPar"          with linespoints ls 1, \
     "data_4elt_ccm.txt" using 1:3 title "KaHIP"             with linespoints ls 2, \
     "data_4elt_ccm.txt" using 1:4 title "ReNUP, {/Symbol d}=0.0"  with linespoints ls 3, \
     "data_4elt_ccm.txt" using 1:5 title "ReNUP, {/Symbol d}=780"   with linespoints ls 4

unset output
