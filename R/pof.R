get_catalog_pof <-
	function( data_name = "pof" , output_dir , ... ){

		pof_ftp <- "ftp://ftp.ibge.gov.br/Orcamentos_Familiares/"
		
		ftp_listing <- readLines( textConnection( RCurl::getURL( pof_ftp ) ) )

		ay <- rev( gsub( "(.*) (.*)" , "\\2" , ftp_listing ) )
	
		# hardcoded removal of 1995-1996
		ay <- ay[ !( ay %in% c( "" , "Pesquisa_de_Orcamentos_Familiares_1995_1996" ) ) ]
	
		second_year <- gsub( "(.*)_([0-9]+)" , "\\2" , ay )
	
		catalog <-
			data.frame(
				full_urls = paste0( pof_ftp , ay , "/Microdados/Dados.zip" ) ,
				period = gsub( "Pesquisa_de_Orcamentos_Familiares_" , "" , ay ) ,
				documentation = paste0( pof_ftp , ay , "/" , ifelse( second_year < 2009 , "Documentacao.zip" , "documentacao.zip" ) ) ,
				output_folder = output_dir ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_pof <-
	function( data_name = "nppes" , catalog , path_to_7za = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){
	
		if( system( paste0( '"' , path_to_7za , '" -h' ) , show.output.on.console = FALSE ) != 0 ) stop( paste0( "you need to install 7-zip.  if you already have it, include a parameter like path_to_7za='" , path_to_7za , "'" ) )
		
		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_urls" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

			print( unzipped_files )




			# convert all column names to lowercase
			# names( x ) <- tolower( names( x ) )

			# save( x , file = catalog[ i , 'output_filename' ] )

			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

