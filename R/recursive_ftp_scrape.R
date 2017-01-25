getlisting <-
	function( dirs ){
		
		tf <- tempfile()
		
		hrefs <- NULL
		
		for( this_dir in dirs ){
		
			download.file( this_dir , tf , mode = 'wb' , quiet = TRUE )
			
			hrefs <- c( hrefs , grep( "a href" , readLines( tf ) , value = TRUE , ignore.case = TRUE ) )
		
		}
	
		file.remove( tf )
	
		hrefs
		
	}

recursive_ftp_scrape <-
	function( directories ){

		final_files <- NULL	

		ftp_home <- gsub( "(.+)//(.+?)/(.*)" , "\\1//\\2/" , directories )

		while( length( directories ) > 0 ){
			
			listing <- getlisting( directories )
			directories <- grep( "directory" , listing , ignore.case = TRUE , value = TRUE )
			files <- listing[ !( listing %in% directories ) ]

			if( length( directories ) > 0 ){
				directories <- gsub( "//" , "/" , directories )
				directories <- gsub( "/./" , "/" , directories )
				directories <- paste0( ftp_home , gsub( "(.*)href=\"(.*)\">(.*)" , "\\2" , directories , ignore.case = TRUE ) )
				directories <- directories[ !( directories %in% paste0( ftp_home , ".." ) ) ]
				directories <- gsub( paste0( ftp_home , "/" ) , ftp_home , directories )
			}

			if( length( files ) > 0 ) {
				files <- gsub( "/./" , "/" , files )
				files <- gsub( "//" , "/" , files )
				files <- paste0( ftp_home , gsub( "(.*)href=\"/(.*)\">(.*)" , "\\2" , files , ignore.case = TRUE ) )
				files <- gsub( paste0( ftp_home , "/" ) , ftp_home , files )
			}

			final_files <- c( final_files , files )

		}
	
		final_files
	}
	
