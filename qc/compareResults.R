orderFactors <- function(reads){

	levelIndex <- pmatch("level",colnames(reads));
	taxonIndex <- pmatch("taxon",colnames(reads));

	reads[which(reads[,taxonIndex]=="Uncertain"),levelIndex] <- "Uncertain";
	reads[which(reads[,taxonIndex]=="Filtered"),levelIndex] <- "Filtered";

	omits <- which(reads[,"level"]=="");
	if(length(omits) > 0 ){
		reads <- reads[-omits,];
	}

	newOrder <- numeric(nrow(reads));
	reads<- cbind(reads,newOrder);
	reads[which(reads[,levelIndex]=="Uncertain"),ncol(reads)] <- 1;
	reads[which(reads[,levelIndex]=="Filtered"),ncol(reads)] <- 2;
	
	reads[which(reads[,levelIndex]=="SUPERKINGDOM"),ncol(reads)] <- 3;
	reads[which(reads[,levelIndex]=="KINGDOM"),ncol(reads)] <- 4;
	reads[which(reads[,levelIndex]=="PHYLUM"),ncol(reads)] <- 5;
	reads[which(reads[,levelIndex]=="CLASS"),ncol(reads)] <- 6;
	reads[which(reads[,levelIndex]=="ORDER"),ncol(reads)] <- 7;
	reads[which(reads[,levelIndex]=="FAMILY"),ncol(reads)] <- 8;
	reads[which(reads[,levelIndex]=="GENUS"),ncol(reads)] <- 9;
	reads[which(reads[,levelIndex]=="SPECIES"),ncol(reads)] <- 10;

	print(paste("no non-missing arguments probably mean that the levels aren't found in this dataset."));
	newFactor <- with(reads,reorder(as.factor(level),reads[,ncol(reads)])); # last arg was newOrder;
	reads[,levelIndex] <- newFactor;

	return(reads);
}

corpus <- read.table("rdp_download_5172seqs.tsv",as.is=T,sep="\t",header=T);
#corpus <- corpus[,-3]; # don't need the sequence here!
#qcmin <- read.table("redos.spuhsdontvote.txt",header=F,as.is=T,sep="\t");

qcmin <- read.table("qc.min2",header=F,as.is=T,sep="\t");
qcmin <- qcmin[,c(1,4,5,7,8,9,10,11,12,13)];

colnames(qcmin) <- c('id','level','taxon','score','topscore','% ident','cov','full','votesfor','votestotal');

results <- merge(corpus,qcmin,by="id");
mismatches <- character(nrow(results));
indices <- c();

for( i in 1:nrow(results )){

	if( results[i,"Taxa"] != results[i,"taxon"] ){
		indices <- append(indices,i);
		mismatches[i] <- "Incongruent";

	} else {
		mismatches[i] <- "Congruent";
	}
}

results <- as.data.frame( results );
errors <- results[indices,];
errors <- orderFactors( errors );

print(paste("there are: ",nrow(errors)," incongruencies."));
filtered <- which(!is.na(pmatch(errors[,"taxon"],"Filtered",dup=T)));
uncertain <- which(!is.na(pmatch(errors[,"taxon"],"Uncertain",dup=T)));

print(paste("there are:",nrow(errors)," errors to review."));
print(paste("there are: ",length(filtered)," filtered reads and", length(uncertain),"uncertain reads"));
print(paste("without review: ", (1-nrow(errors)/nrow(results)),"% of the assignments agree with RDP-II."));

tiff(filename="errorRes.tiff");
myBarPlot <- barplot(table(errors$level)/nrow(errors),ylim=c(0,0.425),xlab="Taxonomic rank.",col="cornflowerblue");
#labels <- toupper(names(table(errors$level)));
#mtext(labels,at=colMeans(myBarPlot),side=1,line=1,cex=0.75);
title("Resolution of mismatches.");
box();
dev.off();

print("recall that 35 of the 41 Uncertain reads are picked up (false negatives) when we permit null genera from the taxonomy database.");
print("review the filtered reads.");
print("of the remaining reads::::");

tiff("figure2.tiff");plot.new();
fig2 <- barplot(c(50,46,45,43,41,38)-30,offset=30,col="cornflowerblue",xlab="Slip-score (%)",ylim=c(30,53));
abline(h=35,lty=3);
mtext(c("Best-score","1","1.5","2","3","5"),at=fig2,side=1,line=1);
title("Slip-score tradeoffs.");
box();
dev.off();


#for( i in 1:nrow(qcredo)){
#	ind <- which(qcmin[,1] == qcredo[i,1]);
#	qcmin[ind,] <- qcredo[i,];
#}
