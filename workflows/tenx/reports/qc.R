suppressPackageStartupMessages({
  require("optparse")
  require("SingleCellExperiment")
  require("stringr")
  require("tidyverse")
  require("scater")
})


# helper function
csv_plot_lib <- function(sce, library_id, 
                          output_dir,
                          max_mito_filtered=20, 
                          max_ribo_filtered=60, 
                          min_features=1000)
{
  # create directory to store libraries within parent output_dir if dir doesn't exist
  if (!file.exists(paste0(output_dir, "/libraries/", library_id))){
    dir.create(paste0(output_dir, "/libraries/", library_id))
  }

  # filter and retrieve cells with good quality.
  keep_rbf <- sce$pct_counts_ribo <= max_ribo_filtered
  keep_mtf <- sce$pct_counts_mito <= max_mito_filtered
  keep_features <- sce$total_features_by_counts >= min_features
  filtered_features <- keep_rbf & keep_mtf & keep_features
  sce$filtered_features <- filtered_features
  sce_filtered <- sce[, filtered_features]
  sce$QC <- ifelse(sce$filtered_features==F,'Low_Quality_Cells',
                           ifelse(sce$filtered_features==T,'Filtered_Cells',NA))

  # create df 
  retrieved_fields <- list()
  retrieved_fields[["Metrics"]] <- "Values **"
  retrieved_fields[["library_id"]] <- library_id
  retrieved_fields[["total_num_cells"]] <- dim(sce)[2]
  retrieved_fields[["total_num_filtered_cells"]] <- dim(sce_filtered)[2]
  retrieved_fields[["pct_good_quality_cells"]] <- paste0(formatC((dim(sce_filtered)[2] * 100) / retrieved_fields$total_num_cells, format = "f", digits = 2 ), "%")

  keep_mt <- sce$pct_counts_mito <= max_mito_filtered
  retrieved_fields[[paste0("num_mito_<=_", max_mito_filtered)]] <- sum(keep_mt)
  retrieved_fields[[paste0("pct_cells_mito_<=_", max_mito_filtered)]] <- paste0(formatC((sum(keep_mt) * 100) / retrieved_fields$total_num_cells, format = "f", digits = 2), "%")

  retrieved_fields[[paste0("num_mito_>_", max_mito_filtered)]] <- (retrieved_fields$total_num_cells - sum(keep_mt))
  retrieved_fields[[paste0("pct_cells_mito_>_", max_mito_filtered)]] <- paste0(formatC(100 - ((sum(keep_mt) * 100) / retrieved_fields$total_num_cells), format = "f", digits = 2), "%")

  retrieved_fields[[paste0("num_ribo_", max_ribo_filtered)]] <- sum(keep_rbf)
  retrieved_fields[[paste0("pct_cells_ribo_", max_ribo_filtered)]] <- paste0(formatC((sum(keep_rbf) * 100) / retrieved_fields$total_num_cells, format = "f", digits = 2), "%")

  retrieved_fields[["median_genes_per_cell_unfiltered"]] <- formatC(median(sce$total_features_by_counts), format = "d")
  retrieved_fields[["median_genes_per_cell_filtered"]] <- formatC(median(sce_filtered$total_features_by_counts), format = "d")
  retrieved_fields[["mean_genes_per_cell_unfiltered"]] <- formatC(mean(sce$total_features_by_counts), format = "d")
  retrieved_fields[["mean_genes_per_cell_filtered"]] <- formatC(mean(sce_filtered$total_features_by_counts), format = "d")

  retrieved_fields[["mean_reads_per_cell_unfiltered"]] <- formatC(mean(sce$total_counts), format = "d")
  retrieved_fields[["mean_reads_per_cell_filtered"]] <- formatC(mean(sce_filtered$total_counts), format = "d")
  retrieved_fields[["max_reads_per_cell_unfiltered"]] <- max(sce$total_counts)
  retrieved_fields[["max_reads_per_cell_filtered"]] <- max(sce_filtered$total_counts)

  qc_df <- as.data.frame(retrieved_fields)
  write.csv(qc_df, file=paste0(output_dir, '/libraries/', library_id, '/QC_', library_id, '.csv'), row.names=F, quote=F)

  # prepare plots
  meta_data <- as.data.frame(colData(sce))
  metrics_ls <- c("total_features_by_counts", "total_counts", 
                  "pct_counts_mito", "pct_counts_ribo")
  ylabels <- c("# Detected Features (Genes > 0)", "Total Counts", 
              "Mito Percent (%)", "Ribo Percent (%)")
  plots <- list()
  idx <- 1

  # violin plot
  for (metrics in metrics_ls){
    plot <- ggplot(meta_data, aes_string(x="QC", y=metrics, color="QC")) + geom_violin()
    plot <- plot + geom_jitter(position=position_jitter(0.3), size=0.3)
    plot <- plot + labs(x="", y=ylabels[idx], title="")
    plot <- plot + theme(legend.title = element_blank(),
                   panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank())
    plots[[idx]] <- plot 
    idx <- idx + 1
  }
  pqc <- cowplot::plot_grid(plotlist=plots,  align = "h", ncol = 2)
  png(paste0(output_dir, '/libraries/', library_id, '/QC_plots_', library_id, ".png"), height = 2*600, width=2*900, res = 2*72)
  print(pqc)

  # scatter plot
  scatter <- ggplot(meta_data, aes_string(x="total_counts", y="total_features_by_counts", colour="total_features_by_counts")) + geom_point()
  scatter <- scatter + geom_jitter(position=position_jitter(0.3), size=0.3)
  scatter <- scatter + labs(x="Total Counts", y="# Detected Features (Genes > 0)")
  scatter <- scatter + scale_colour_continuous(name="Detected")
  scatter <- scatter + theme(panel.grid.major = element_blank(), 
                            panel.grid.minor = element_blank(), panel.border = element_blank())
  png(paste0(output_dir, '/libraries/', library_id, '/QC_scatter_', library_id, ".png"), height = 600, width=900, res = 2*72)
  print(scatter)

  dev.off()
  print("Successfully generated pngs.")
}


# This function calls a helper function 
# csv_plot_lib to assist in generating library-specific reports and plots required as in JIRA ticket.
generate_QC_reports_lib <- function(library_id, input_dir, output_dir)
{
  print(paste("Processing library:",library_id))

  allData <- list.files(input_dir, recursive = TRUE, full.names = TRUE)
  rdata <- grep(paste0(library_id, ".rdata"), allData, value = TRUE)
  if (length(rdata)) {
    print(paste("Generating reports and plots for the library:", library_id))
    lib_data <- readRDS(rdata)
    csv_plot_lib(lib_data, library_id, output_dir, 20, 60, 1000)
  } 
  else {
    print(paste("Error locating .rdata file for library id", library_id))
  }
}


################################################################################################################################################################
# main call to generate_QC_reports functions.
# takes in 3 arguments:
## library_id: the library id of the library that is currently of interest. e.g. SCRNA10X_SA_CHIP0142_002
## input_dir: the local input directory of the .rdata files that were fetched from Azure Blob.
## output_dir: the output directory of the desired .csv and .png files.


option_list <- list(make_option(c("-l", "--library_id"), type="character", default=NULL, help="library_id", metavar="character"),
                    make_option(c("-i", "--input_dir"), type="character", default=NULL, help="input_dir", metavar="character"),
                    make_option(c("-o", "--output_dir"), type="character", default=NULL, help="output_dir", metavar="character"))

opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)

if (!file.exists(paste0(opt$output_dir, "/libraries"))){
  dir.create(paste0(opt$output_dir, "/libraries"))
}

generate_QC_reports_lib(opt$library_id, opt$input_dir, opt$output_dir)
