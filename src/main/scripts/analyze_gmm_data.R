library(GenomicRanges)
library(rtracklayer)

wdd <- '/Users/cwhelan/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip'
testname <- 'newdiscfilt_as175_bt2_nomapq_nofilt_readGroupsBt2scoreMinL01_None_None_2500_25_3_sam_2000_fbbecea_175'

l1 <- read.table(paste(wd, '/', testname, '_l1.wig.gz', sep=""), skip=2, col.names=c("loc", "l1"))
w <- read.table(paste(wd, '/', testname, '_w0.wig.gz', sep=""), skip=2, col.names=c("loc", "w"))
mu <- read.table(paste(wd, '/', testname, '_mu1.wig.gz', sep=""), skip=2, col.names=c("loc", "mu"))
lrHet <- read.table(paste(wd, '/', testname, '_lrHet.wig.gz', sep=""), skip=2, col.names=c("loc", "lrHet"))
lrHom <- read.table(paste(wd, '/', testname, '_lrHom.wig.gz', sep=""), skip=2, col.names=c("loc", "lrHom"))

features <- merge(l1, w, by="loc")
features <- merge(features, mu, by="loc")
features <- merge(features, lrHet, by="loc")
features <- merge(features, lrHom, by="loc")

windows <- GRanges(seqnames="2", ranges=IRanges(start=seq(0,242751150,by=25), end=seq(24, 242751174, by=25)), strand="*")

hap1file <- '/l2/users/whelanch/gene_rearrange/data/jcvi/HuRef.homozygous_indels_inversion.061109.chr2.Deletions.sim_hap1.bed.gz'
hap2file <- '/l2/users/whelanch/gene_rearrange/data/jcvi/HuRef.homozygous_indels_inversion.061109.chr2.Deletions.sim_hap2.bed.gz'

hap1 <- import(hap1file, asRangedData=FALSE)
hap2 <- import(hap2file, asRangedData=FALSE)

elementMetadata(windows)[,"genotype"] <- 0

hap1gt50 <- hap1[end(hap1) - start(hap1) >= 50]
hap2gt50 <- hap2[end(hap2) - start(hap2) >= 50]

elementMetadata(windows)[as.matrix(findOverlaps(hap1gt50, windows))[,2],"genotype"] <- 1
elementMetadata(windows)[as.matrix(findOverlaps(hap2gt50, windows))[,2],"genotype"] <- 2

genotype <- data.frame(loc=start(windows), genotype=elementMetadata(windows)[,"genotype"])


features <- merge(features, genotype, by="loc")

features$sv <- features$genotype != 0

tryit <- function(x,y) {

    #testindex <- 1:(2 * nrow(x)/3)
    testindex=sample(1:dim(x)[1],dim(x)[1]/3)

    xtrain <- x[-testindex,]
    ytrain <- y[-testindex]

    xtrain <- scale(xtrain,center=TRUE,scale=TRUE)

    l <- LiblineaR(as.matrix(xtrain), as.matrix(ytrain), type=1, cost=1.6, verbose=TRUE)

    xtest <- x[testindex,]
    ytest <- y[testindex]
    xtest=scale(xtest,attr(xtrain,"scaled:center"),attr(xtrain,"scaled:scale"))

        pr=FALSE
    p=predict(l,xtest,proba=pr,decisionValues=TRUE)

    res=table(p$predictions,ytest)
    print(res)

    l
}

l <- tryit(features[,2:6], features[,7])

l <- tryit(features[,2:6], features[,8])


library(LiblineaR)


rm(w)
rm(mu)
rm(l1)
rm(lrHet)
rm(lrHom)
rm(windows)

attach(features)

index <- 1:nrow(features)
testindex <- sample(index, trunc(length(index)/3))
testset <- features[testindex,]
trainset <- features[-testindex,]

svm.model <- svm(genotype ~ l1 + w + mu + lrHet + lrHom, data = trainset, cost = 100, gamma = 1)

gc()


cb.predictions.file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/new3_readGroupsRazerS3_new3_i94_s99_m1000_None_None_25000_25_sam_3_b0b0757_-1_gt_114714_filt.bed'

cb.predictions <- read.table(cb.predictions.file, skip=1)
names(cb.predictions) <- c("chr", "start", "end", "num", "maxLrHet", "avgMu1", "avgLrHom", "avgW0", "avgCleanCov", 
                           "avgC1Mem", "avgC2Mem", "avgWeightedC1", "avgWeightedC2")

cbRanges <- GRanges(seqnames=cb.predictions$chr, ranges=IRanges(start=cb.predictions$start, end=cb.predictions$end))

hap1file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/HuRef.homozygous_indels_inversion.061109.chr2.Deletions.sim_hap1.bed.gz'
hap2file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/HuRef.homozygous_indels_inversion.061109.chr2.Deletions.sim_hap2.bed.gz'

hap1 <- import(hap1file, asRangedData=FALSE)
hap2 <- import(hap2file, asRangedData=FALSE)

mcols(hap1)$hap <- 1
mcols(hap2)$hap <- 2

hap1 <- hap1[-1 * as.matrix(findOverlaps(hap1,hap2))[,1],]

trueDels <- c(hap1,hap2)

trueDelsGt20 <- trueDels[end(trueDels) - start(trueDels) >= 20]

mcols(cbRanges)$geno <- 0

trueOverlaps <- as.matrix(findOverlaps(cbRanges, trueDelsGt20))
                                          
genos <- lapply(split(data.frame(trueOverlaps), trueOverlaps[,1]), 
       function (x) {          
          trueDelsGt20[x$subjectHits,][order(end(trueDelsGt20[x$subjectHits]) - 
                                          start(trueDelsGt20[x$subjectHits]), decreasing=TRUE)][1]$hap})

mcols(cbRanges[as.numeric(names(genos))])$geno <- unlist(genos)

cb.predictions$geno <- mcols(cbRanges)$geno


