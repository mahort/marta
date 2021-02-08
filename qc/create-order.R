orderFactors <- function(reads){

	levelIndex <- pmatch("level",colnames(reads));
	taxonIndex <- pmatch("taxon",colnames(reads));

	reads[which(reads[,taxonIndex]=="Uncertain"),levelIndex] <- "Uncertain";
	reads[which(reads[,taxonIndex]=="Filtered"),levelIndex] <- "Filtered";

	#nosig <- which(reads[,levelIndex]=="No significant hits.");
	#scores[-nosig,] -> sighits;
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
	
	newFactor <- with(reads,reorder(as.factor(level),reads[,ncol(reads)])); # last arg was newOrder;
	reads[,levelIndex] <- newFactor;

	return(reads);
}

