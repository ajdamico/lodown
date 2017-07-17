get_catalog_mapd_landscape <-
	function( data_name = "mapd_landscape" , output_dir , ... ){

		landscape_url <- "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/index.html?redirect=/PrescriptionDrugCovGenIn/"

		prefix <- "https://www.cms.gov/"
		
		all_links <- rvest::html_nodes( xml2::read_html( landscape_url ) , xpath = '//li/a' )

		all_names <- rvest::html_text( all_links )
	
		all_links <- gsub( '(.*)href=\"' , prefix , all_links )
		all_links <- gsub( "\">(.*)" , "" , all_links )

		zipped_link_nums <- grep( "\\.zip$" , all_links , ignore.case = TRUE )
		
		zip_links <- all_links[ zipped_link_nums ]
		zip_names <- gsub( "( +?)\\t(.*)" , "" , all_names[ zipped_link_nums ] )
		
		these_zips <-
			data.frame(
				zip_names = zip_names , 
				full_url = zip_links ,
				year = substr( zip_names , 1 , 4 ) ,
				stringsAsFactors = FALSE
			)
		
		early_lsc <- subset( these_zips , zip_names == "2007-2012 PDP, MA, and SNP Landscape Files" )
		
		early_lsc$year <- NULL
		
		these_zips <- subset( these_zips , !( zip_names %in% "2007-2012 PDP, MA, and SNP Landscape Files" ) )

		these_zips <-
			rbind( 
				these_zips , 
				merge( expand.grid( year = 2007:2012 ) , early_lsc )
			)
		
		ma_landscape_zips <- 
			subset( these_zips , grepl( "MA(.*)Landscape|Landscape and" , zip_names ) )
		
		ma_landscape_zips$type <- "MA"
		
		pdp_landscape_zips <- 
			subset( these_zips , grepl( "PDP(.*)Landscape|Landscape and" , zip_names ) )
		
		pdp_landscape_zips$type <- "PDP"
		
		snp_landscape_zips <- 
			subset( these_zips , grepl( "SNP(.*)Landscape|Landscape and" , zip_names ) )
		
		snp_landscape_zips$type <- "SNP"
		
		mmp_landscape_zips <- 
			subset( these_zips , grepl( "MMP(.*)Landscape|Landscape and" , zip_names ) )
		
		mmp_landscape_zips$type <- "MMP"
		
		this_catalog <-
			rbind(
				ma_landscape_zips ,
				pdp_landscape_zips ,
				snp_landscape_zips ,
				mmp_landscape_zips
			)
			
		this_catalog$output_filename <-
			paste0( 
				output_dir , 
				"/" , 
				this_catalog$year , 
				"/" , 
				tolower( this_catalog$type ) , 
				".rds" 
			)
		
		this_catalog <- subset( this_catalog , !( type == 'SNP' & year == 2007 ) )
		
		this_catalog[ order( this_catalog$year ) , ]
	}


lodown_mapd_landscape <-
	function( data_name = "mapd_landscape" , catalog , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , filesize_fun = 'httr' )

			unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( catalog[ i , 'output_filename' ] ) )

			second_round <- grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			for( this_zip in second_round ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = np_dirname( catalog[ i , 'output_filename' ] ) ) )
			
			third_round <- grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			third_round <- setdiff( third_round , second_round )
			
			for( this_zip in third_round ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = np_dirname( catalog[ i , 'output_filename' ] ) ) )
			
			# find relevant csv files
			these_csv_files <- unzipped_files[ grep( paste0( catalog[ i , 'year' ] , "(.*)" ,  catalog[ i , 'type' ] , "(.*)\\.(csv|CSV)" ) , basename( unzipped_files ) ) ]
			
			out <- NULL
			
			for( this_csv in these_csv_files ){
				
				first_twenty_lines <- readLines( this_csv , n = 20 )
				
				which_state_county <- min( grep( "State" , first_twenty_lines ) )
				
				csv_df <- data.frame( readr::read_csv( this_csv , skip = ( which_state_county - 1 ) ) )
				
				if( grepl( "sanction" , this_csv , ignore.case = TRUE ) ) csv_df$sanctioned <- TRUE else csv_df$sanctioned <- FALSE
				
				out <- rbind( out , csv_df )
				
			}
			
			names( out ) <- tolower( names( out ) )
			
			saveRDS( out , file = catalog[ i , 'output_filename' ] )

			file.remove( unzipped_files )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
			
		}

		catalog

	}

