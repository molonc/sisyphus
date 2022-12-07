chasmbrain <- function(input) {
  
  metrics <- read.csv(input)

  # metrics <- subset(metrics, experimental_condition == "A")
  
  metrics$reads <- metrics$total_mapped_reads_hmmcopy
  metrics$reads[is.na(metrics$reads)] <- metrics$total_mapped_reads[is.na(metrics$reads)]
  metrics$reads[is.na(metrics$reads)] <- 0
  
  breaks <- c(0, 0.1, 0.4, 0.75, 0.95, 1.0)
  labels <- c("FAIL", "LQ", "MID", "PASS", "WOW")
  metrics$bins <- cut(metrics$quality, breaks, labels, include.lowest = TRUE)
  
  before_summary <- metrics %>% group_by(bins) %>% summarise(counts = length(quality), mean_reads = mean(reads))
  # COMPUTING CELLS + READS per quality group
  bins_cells <- before_summary %>% select(bins, counts) %>% spread(bins, counts, sep = "_")
  colnames(bins_cells) <- sub("bins", "cells", colnames(bins_cells))
  bins_reads <- before_summary %>% select(bins, mean_reads) %>% spread(bins, mean_reads, sep = "_")
  colnames(bins_reads) <- sub("bins", "reads", colnames(bins_reads))
  bins_stats <- merge(bins_cells, bins_reads)
  
  # REDO EVERYTHING, but as PERCENTAGES NOW!
  totals <- metrics %>% summarise(total_reads = sum(reads), total_cells = length(quality))
  
  bins_cells2 <- bins_cells / totals$total_cells
  bins_reads2 <- bins_reads / totals$total_reads
  
  bins_stats2 <- merge(bins_cells2, bins_reads2)
  bins_stats2 <- merge(bins_stats2, totals)
  
  # https://en.wikipedia.org/wiki/Underdetermined_system
  # PREVENT UNDERDETERMINED SYSTEM
  bins_stats2 <- select(bins_stats2, c(-cells_FAIL, -reads_FAIL))

  x <- as.matrix(bins_stats2)
  
  load("recovery_predictor.Rds")
  pred <- predict(recovery_predictor, newx = x, s = "lambda.min")
  # print(pred)
  
  output <- bins_stats2
  output$pred <- pred[, 1]
  
  output$computebefore <- output$total_cells * (output$cells_PASS + output$cells_WOW)
  output$predafter <- output$computebefore * (1 + output$pred)
  
  # print(output)
  cat(paste("total cells:", output$total_cells), sep = "\n")
  cat(paste("good cells now:", output$computebefore), sep = "\n")
  cat(paste("predicted good % increase", round(output$pred * 100, 2)), sep = "\n")
  cat(paste("predicted good cells after:", round(output$predafter)), sep = "\n")
  
  # > bins_stats2
  #   cells_LQ cells_MID cells_PASS  cells_WOW     reads_LQ    reads_MID
  # 1 0.510296 0.2265122  0.1332046 0.04054054 0.0005984526 0.0007275572
  #     reads_PASS   reads_WOW total_reads total_cells
  # 1 0.0009409955 0.001089758   381997402        1554
}