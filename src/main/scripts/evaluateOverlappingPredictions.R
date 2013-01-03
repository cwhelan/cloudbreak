library(rtracklayer)
library(ggplot2)

truth.file <- '/Users/cwhelan/Documents/gene_rearrange/svpipeline/venter_chr2_sim/HuRef.homozygous_indels_inversion.061109.chr2.Deletions.gff'

cb.predictions.file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/new3_readGroupsRazerS3_new3_i94_s99_m1000_None_None_25000_25_sam_3_b0b0757_-1_gt_114714_filt.bed'
#bd.predictions.file <- '~/Documents/gene_rearrange/sv/jcvi_sim/chr2/novoalign_tier1_sort_clean_rmdup_35_2_4.bd_out.txt'

cb.predictions.file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/maxins_25000_sdseqfilter_fdr20.bed'
#bd.predictions.file <- '~/Documents/gene_rearrange/sv/jcvi_sim/chr2/novoalign_tier1_sort_clean_rmdup_35_2_4.bd_out.txt'

bd.score.threshold <- 53

# Returns "N" if neither match, "B" if both match, predictor.1 if only calls.1 matches, or predictor.2 if only calls.2 matches
agreement <- function(true.region, calls.1, calls.2, predictor.1, predictor.2) {
  c1.overlaps <- findOverlaps(true.region, calls.1)
  c2.overlaps <- findOverlaps(true.region, calls.2)
  num.overlaps <- (length(c1.overlaps) > 0) + (length(c2.overlaps) > 0)
  if (num.overlaps == 0) return("Neither")
  if (num.overlaps == 2) return("Both")
  if (length(c1.overlaps) > 0) return(predictor.1)
  if (length(c2.overlaps) > 0) return(predictor.2)
  "X"
}

true.df <- read.table(truth.file)

true.intervals <- GRanges(seqnames=true.df$V1, ranges=IRanges(start=true.df$V4, end=true.df$V5), strand=true.df$V7)

cb.predictions <- import.bed(cb.predictions.file, asRangedData=FALSE)

bd.predictions.df <- read.table(bd.predictions.file)

names(bd.predictions.df) <- c("Chr1","Pos1","Orientation1","Chr2","Pos2","Orientation2","Type","Size","Score","num_Reads","num_Reads_lib","DOC")

bd.deletions.df <- bd.predictions.df[which(bd.predictions.df$Type == "DEL"),]
bd.deletions.df <- bd.deletions.df[which(bd.deletions.df$Chr1 == bd.deletions.df$Chr2),]
bd.deletions.df <- bd.deletions.df[which(bd.deletions.df$Pos2 - bd.deletions.df$Pos1 <= 25000),]

bd.deletions.above.thresh <- bd.deletions.df[which(bd.deletions.df$Score > bd.score.threshold),]

bd.predictions <- GRanges(seqnames=bd.deletions.above.thresh$Chr1, 
                          ranges=IRanges(start=bd.deletions.above.thresh$Pos1, end=bd.deletions.above.thresh$Pos2),
                          strand="*")

agreements <- sapply(seq(1,length(true.intervals)), function(x) {agreement(true.intervals[x], cb.predictions, bd.predictions, "Cloudbreak", "Breakdancer")})

true.sizes <- end(true.intervals) - start(true.intervals)
true.size.classes <- cut(true.sizes, breaks=c(0,100, 150, 200, 250, 300, 500, 1000, 2000, 5000, 20000), labels=c("50-100", "100-150", "150-200", "200-250", "250-300", "300-500","500-1000","1000-2000","2000-5000","5000-20000"))
agreements.df <- data.frame(size=true.sizes, size.class=true.size.classes, agreements=agreements)

ggplot(agreements.df, aes(size.class)) + geom_bar() + facet_wrap(~ agreements) 

delly.predictions.file <- '~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/delly_q0_c5}_del.txt'
delly.score.threshold <- 3

delly.deletions.df <- read.table(delly.predictions.file)
names(delly.deletions.df) <- c("Chr","start","end","Size","Score","mq","name")

delly.deletions.df <- delly.deletions.df[which(delly.deletions.df$Size <= 25000),]
delly.deletions.above.thresh <- delly.deletions.df[which(delly.deletions.df$Score >= delly.score.threshold),]

delly.predictions <- GRanges(seqnames=delly.deletions.above.thresh$Chr, 
                          ranges=IRanges(start=delly.deletions.above.thresh$start, end=delly.deletions.above.thresh$end),
                          strand="*")

agreements <- sapply(seq(1,length(true.intervals)), function(x) {agreement(true.intervals[x], cb.predictions, delly.predictions, "Cloudbreak", "DELLY")})
agreements.df <- data.frame(size=true.sizes, size.class=true.size.classes, agreements=agreements)

pdf('~/Documents/gene_rearrange/svpipeline/CB_DELLY_SIZE_AGREEMENT.pdf')
ggplot(agreements.df, aes(size.class)) + geom_bar() + scale_x_discrete(name="Deletion Size")  + facet_wrap(~ agreements) + theme(axis.text.x=theme_text(angle=-90))
dev.off()





