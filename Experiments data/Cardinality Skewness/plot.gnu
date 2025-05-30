set terminal pngcairo size 800,600 enhanced font 'Arial,10'
set output 'comparison_plot.png'

set title 'Performance Comparison by Imbalance'
set xlabel 'Skewness'
set ylabel 'Improvement (%)'

set grid
set key outside

plot 'results.dat' using 1:2 with linespoints title 'KaMinPar', \
     '' using 1:3 with linespoints title 'KaHIP', \
     '' using 1:4 with linespoints title 'Deep NUGP, ε=0', \
     '' using 1:5 with linespoints title 'Deep NUGP, ε=50'
