





brokenBar2 <- function(dataset){
	plot(seq(from = 0, to = 2, by = 0.5), # X
		seq(from = 0, to = 4, by = 1), # Y --- *** X and Y must have the same length ***
		axes=FALSE, # set up but don't plot the axes 
		type="n", xlab="X", ylab="Y");

# Add break lines along x-axis
	text("l", x = 1.20, y = 0, srt = -45, font = 2)
	text("l", x = 1.25, y = 0, srt = -45, font = 2)
	lines(x = c(0, 1.19), y = c(0,0), lwd = 2)
	lines(x = c(1.25, 4.0), y = c(0,0), lwd = 2)

# Add break lines along y-axis
	text("l", y = 1.15, x = 0, srt = -45, font = 2)
	text("l", y = 1.25, x = 0, srt = -45, font = 2)
	lines(y = c(0, 1.15), x = c(0,0), lwd = 2)
	lines(y = c(1.27, 4), x = c(0,0), lwd = 2)

# Add x-axis (tick marks and labels)
	mtext("l", side = 1, line = -1.25, at = seq(0, 2, 0.5), font = 2)
	axis(side = 1, at = seq(0, 2, 0.5), line = -1, tick = FALSE,labels = c("0.0", "0.5", "1.0", "5.5", "6.0"))

# Add y-axis (tick marks and labels)
	mtext("l", side = 2, line = -1.20, at = seq(0, 4, 0.5), font = 2)
	axis(side = 2, at = seq(0, 4, 0.5), line = -1, tick = FALSE,
	labels = as.character(c(seq(from = 0, to = 4, by = 2),
	seq(from = 100, to = 200, by = 20))))
}

brokenBarplot <- function(dataset){
	plot(seq(from = 0, to = 5, by = 1), # X
		seq(from = 0, to = 5, by = 1), # Y --- *** X and Y must have the same length ***
		axes=FALSE, # set up but don't plot the axes 
		type="n", xlab="X", ylab="Y");

# Add break lines along y-axis
	text("l", y = 2.08, x = 0, srt = 45, font = 2)
	text("l", y = 2.28, x = 0, srt = 45, font = 2)
	lines(y = c(0, 2.08), x = c(0,0), lwd = 2)
	lines(y = c(2.28, 5), x = c(0,0), lwd = 2)

# Add y-axis (tick marks and labels)
mtext("l", side = 2, line = -1.20, at = as.numeric(c(seq(0, 2,0.25),seq(2.5,5,.25))), font = 2)
	axis(side = 2, at = seq(0, 5, 0.25), line = -1, tick = FALSE,
			labels = as.character(c(seq(from = 0, to = 200, by = 25),
									seq(from = 4625, to = 4900, by = 25))))

	
}


broken <- function(dataset){
	plot(seq(from = 0, to = 3.5, by = 1), # X
		seq(from = 0, to = 3.5, by = 1), # Y --- *** X and Y must have the same length ***
		axes=FALSE, # set up but don't plot the axes 
		type="n", xlab="X", ylab="Y");

# Add break lines along y-axis
	text("l", y = 2.02, x = 0, srt = 45, font = 2)
	text("l", y = 2.17, x = 0, srt = 45, font = 2)
	lines(y = c(0, 2.02), x = c(0,0), lwd = 2)
	lines(y = c(2.17, 3.5), x = c(0,0), lwd = 2)

# Add y-axis (tick marks and labels)
	mtext("l", side = 2, line = -1.3, at = as.numeric(c(seq(0, 2,0.25),seq(2.25,3.5,.25))), font = 10,cex.lab=0.4)
	axis(side = 2, at = as.character(c(seq(0, 2, 0.25),seq(2.25,3.5,.25))), line=-1,tick = FALSE,
			labels = as.character(c(seq(from = 0, to = 200, by = 25),
			seq(from = 4750, to = 4875, by = 25))))
}
