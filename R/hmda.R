get_catalog_hmda <-
	function( data_name = "hmda" , output_dir , ... ){

		hmda_table <- rvest::html_table( xml2::read_html( "https://www.ffiec.gov/hmda/hmdaflat.htm" ) , fill = TRUE )
		
		pmic_table <- rvest::html_table( xml2::read_html( "https://www.ffiec.gov/hmda/pmicflat.htm" ) , fill = TRUE )
		
		latest_hmda_year <- max( unlist( sapply( hmda_table , function( z ) suppressWarnings( as.numeric( names( z ) ) ) ) ) , na.rm = TRUE )
		
		latest_pmic_year <- max( unlist( sapply( pmic_table , function( z ) suppressWarnings( as.numeric( names( z ) ) ) ) ) , na.rm = TRUE )
		
		cat_hmda_lar <-
			data.frame(
				year = 2006:latest_hmda_year ,
				type = 'hmda_lar' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/" , "LAR/National" , "/20" , 2006:latest_hmda_year , "HMDA" , "lar%20-%20National" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_lar <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_lar' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "OTHER" , "/20" , 2006:latest_pmic_year , "PMIC" , "lar%20-%20National" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_inst <-
			data.frame(
				year = 2006:latest_hmda_year ,
				type = 'hmda_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/" , "LAR/National" , "/20" , 2006:latest_hmda_year , "HMDA" , "institutionrecords" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_inst <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "OTHER" , "/20" , 2006:latest_pmic_year , "PMIC" , "institutionrecords" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_reporter <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/20" , 2007:latest_hmda_year , "HMDA" , "ReporterPanel.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_reporter <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/OTHER/20" , 2007:latest_pmic_year , "PMIC" , "ReporterPanel.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		
		cat_hmda_msa <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/20" , 2007:latest_hmda_year , "HMDA" , "MSAOffice.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_msa <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/20" , 2007:latest_pmic_year , "HMDA" , "MSAOffice.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		catalog <- rbind( cat_hmda_lar , cat_pmic_lar , cat_hmda_inst , cat_pmic_inst , cat_hmda_reporter , cat_pmic_reporter , cat_hmda_msa , cat_pmic_msa )

		catalog$dbfolder <- paste0( output_dir , "/MonetDB" )
		
		catalog$db_tablename <- paste0( catalog$type , "_" , catalog$year )
		
		catalog
	}


lodown_hmda <-
	function( data_name = "hmda" , catalog , path_to_7za = '7za' , ... ){

		if( ( .Platform$OS.type != 'windows' ) && ( system( paste0('"', path_to_7za , '" -h' ) , show.output.on.console = FALSE ) != 0 ) ) stop( "you need to install 7-zip.  if you already have it, include a path_to_7za='/directory/7za' parameter" )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )






			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

