get_catalog_mapdlandscape <-
	function( data_name = "mapdlandscape" , output_dir , ... ){

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
				data_name = zip_names , 
				full_url = zip_links ,
				year = substr( zip_names , 1 , 4 ) ,
				stringsAsFactors = FALSE
			)
		
		early_lsc <- subset( these_zips , data_name == "2007-2012 PDP, MA, and SNP Landscape Files" )
		
		early_partd <- subset( these_zips , data_name == "2006-2012 Plan and Premium Information for Medicare Plans Offering Part D" )
		
		early_lsc$year <- early_partd$year <- NULL
		
		these_zips <- 
			subset( 
				these_zips , 
				!( data_name %in% "2007-2012 PDP, MA, and SNP Landscape Files" ) &
				!( data_name %in% "2006-2012 Plan and Premium Information for Medicare Plans Offering Part D" ) 
			)

		these_zips <-
			rbind( 
				these_zips , 
				merge( expand.grid( year = 2007:2012 ) , early_lsc ) ,
				merge( expand.grid( year = 2006:2012 ) , early_partd )
			)
		
		ma_landscape_zips <- 
			subset( these_zips , grepl( "MA(.*)Landscape|Landscape and" , data_name ) )
		
		ma_landscape_zips$type <- "MA"
		
		pdp_landscape_zips <- 
			subset( these_zips , grepl( "PDP(.*)Landscape|Landscape and" , data_name ) )
		
		pdp_landscape_zips$type <- "PDP"
		
		snp_landscape_zips <- 
			subset( these_zips , grepl( "SNP(.*)Landscape|Landscape and" , data_name ) )
		
		snp_landscape_zips$type <- "SNP"
		
		mmp_landscape_zips <- 
			subset( these_zips , grepl( "MMP(.*)Landscape|Landscape and" , data_name ) )
		
		mmp_landscape_zips$type <- "MMP"
		
		part_d_landscape_zips <- 
			subset( these_zips , grepl( "Medicare Plans Offering Part D" , data_name ) )
		
		part_d_landscape_zips$type <- "PartD"
		
		this_catalog <-
			rbind(
				ma_landscape_zips ,
				pdp_landscape_zips ,
				snp_landscape_zips ,
				mmp_landscape_zips ,
				part_d_landscape_zips
			)
			
		this_catalog$output_filename <-
			paste0( 
				output_dir , 
				"/" , 
				this_catalog$year , 
				" " , 
				tolower( this_catalog$type ) , 
				".rds" 
			)
		
		this_catalog <- subset( this_catalog , !( type == 'SNP' & year == 2007 ) & !( type == 'MMP' & year == 2013 ) )
		
		stopifnot( nrow( this_catalog ) == nrow( unique( this_catalog[ c( 'type' , 'year' ) ] ) ) )
		
		this_catalog[ order( this_catalog$year ) , ]
	}


lodown_mapdlandscape <-
	function( data_name = "mapdlandscape" , catalog , ... ){

		on.exit( print( catalog ) )
		
		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( catalog[ i , 'output_filename' ] ) )

			# the 2013, 2014, and 2015 files have tmi
			if( catalog[ i , 'type' ] == 'PartD' & catalog[ i , 'year' ] %in% 2013:2015 ){
				files_to_keep <- unzipped_files[ grep( 'Premium' , basename( unzipped_files ) ) ]
				file.remove( unzipped_files[ !( unzipped_files %in% files_to_keep ) ] )
				unzipped_files <- unzipped_files[ ( unzipped_files %in% files_to_keep ) ]
			}
			
			second_round <- grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			for( this_zip in second_round ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = file.path( np_dirname( catalog[ i , 'output_filename' ] ) , gsub( "\\.(.*)" , "" , basename( this_zip ) ) ) ) )
			
			third_round <- grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			third_round <- setdiff( third_round , second_round )
			
			for( this_zip in third_round ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = file.path( np_dirname( catalog[ i , 'output_filename' ] ) , gsub( "\\.(.*)" , "" , basename( this_zip ) ) ) ) )
			
			# find relevant csv files
			if( catalog[ i , 'type' ] == 'PartD' ){
				folder_with_plan_report <- unique( dirname( grep( paste0( catalog[ i , 'year' ] , " Plan Report" ) , unzipped_files , value = TRUE ) ) )
				stopifnot( length( folder_with_plan_report ) == 1 )
				these_csv_files <- unzipped_files[ ( dirname( unzipped_files ) == folder_with_plan_report ) & grepl( "\\.(csv|CSV)$" , unzipped_files ) & !grepl( "ImportantNotes" , unzipped_files , ignore.case = TRUE ) ]
			} else {
				these_csv_files <- unzipped_files[ grep( paste0( catalog[ i , 'year' ] , "(.*)" ,  catalog[ i , 'type' ] , "(.*)\\.(csv|CSV)" ) , basename( unzipped_files ) ) ]
			}
			
			out <- NULL
			
			for( this_csv in these_csv_files ){
				
				first_twenty_lines <- read.csv( this_csv , nrows = 20 , stringsAsFactors = FALSE )

				which_state_county <- min( which( first_twenty_lines[ , 1 ] == 'State') )
				
				# hardcode a mistaken file in the 2015 part d plan & premium information
				if( basename( this_csv ) == "508_NebraskatoWyoming 03182015.csv" ){
				
					csv_df <- data.frame( readxl::read_excel( grep( "03182015\\.xls" , unzipped_files , value = TRUE ) , sheet = 2 , skip = 3 ) )
					csv_df[ is.na( csv_df ) ] <- ""
					
				} else {
				
					csv_df <- read.csv( this_csv , skip = which_state_county , stringsAsFactors = FALSE )
				
				}
				
				csv_df <- csv_df[ , !grepl( "^X" , names( csv_df ) ) ]
				
				if( grepl( "sanction" , this_csv , ignore.case = TRUE ) ) csv_df$sanctioned <- TRUE else csv_df$sanctioned <- FALSE
				
				if( !is.null( out ) & ( "Type.of..Additional.Coverage.Offered.in.the.Gap" %in% names( csv_df ) ) ) out[ , "Type.of..Additional.Coverage.Offered.in.the.Gap" ] <- NA
				
				out <- rbind( out , csv_df )
				
			}
			
			names( out ) <- tolower( names( out ) )
			
			out <- unique( subset( out , contract.id != '' ) )
			
			names( out ) <- gsub( " " , "_" , stringr::str_trim( gsub( "( +)" , " " , gsub( "\\." , " " , names( out ) ) ) ) )
			
			if( any( !( out$state %in%  c( datasets::state.name , "Washington D.C." , "Puerto Rico" , "Guam" , "Northern Mariana Islands" , "American Samoa" , "Virgin Islands" ) ) ) ) stop( "illegal state name" )
			
			out$year <- catalog[ i , 'year' ]
			
			catalog[ i , 'case_count' ] <- nrow( out )
			
			saveRDS( out , file = catalog[ i , 'output_filename' ] )

			file.remove( unzipped_files )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
			
		}

		on.exit()
		
		catalog

	}

