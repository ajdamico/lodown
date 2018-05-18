get_catalog_timss <-
	function( data_name = "timss" , output_dir , ... ){

		catalog <-
			data.frame(
				year = c( 1995 , 1999 , 2003 , 2007 , 2011 , 2015 ) ,
				stringsAsFactors = FALSE
			)

		catalog$output_folder <- paste0( output_dir , "/" , catalog$year , "/" )
		
		catalog$dbfile <- paste0( output_dir , "/SQLite.db" )
		
		catalog

	}


lodown_timss <-
	function( data_name = "timss" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			# # # # # # # # # # # #
			# download all files  #
			# # # # # # # # # # # #

			c99 <- c( "aus" , "bfl" , "bgr" , "can" , "chl" , "twn" , "cyp" , "cze" , "eng" , "fin" , "hkg" , "hun" , "idn" , "irn" , "isr" , "ita" , "jpn" , "jor" , "kor" , "lva" , "ltu" , "mkd" , "mys" , "mda" , "mar" , "nld" , "nzl" , "phl" , "rom" , "rus" , "sgp" , "svk" , "svn" , "zaf" , "tha" , "tun" , "tur" , "usa" )

			c95_1 <- c("AUS", "AUT", "CAN", "CYP", "CSK", "GBR", "GRC", "HKG", "HUN", "ISL", "IRN", "IRL", "ISR", "JPN", "KOR", "KWT", "LVA", "NLD", "NZL", "NOR", "PRT", "SCO", "SGP", "SVN", "THA", "USA")
			c95_2 <- c("AUS", "AUT", "BFL", "BFR", "BGR", "CAN", "COL", "CYP", "CSK", "DNK", "GBR", "FRA", "DEU", "GRC", "HKG", "HUN", "ISL", "IRN", "IRL", "ISR", "JPN", "KOR", "KWT", "LVA", "LTU", "NLD", "NZL", "NOR", "PHL", "PRT", "ROM", "RUS", "SCO", "SGP", "SLV", "SVN", "ZAF", "ESP", "SWE", "CHE", "THA", "USA")
			c95_3 <- c("AUS", "AUT", "CAN", "CYP", "CSK", "DNK", "FRA", "DEU", "GRC", "HUN", "ISL", "ISR", "ITA", "LVA", "LTU", "NLD", "NZL", "NOR", "RUS", "SVN", "ZAF", "SWE", "CHE", "USA")

			# specify the pathway to each and every spss data set to download.
			ftd <-
				c(
					paste0( "https://timss.bc.edu/timss2015/international-database/downloads/T15_G4_SPSSData_pt" , 1:3 , ".zip" ) ,
					paste0( "https://timss.bc.edu/timss2015/international-database/downloads/T15_G8_SPSSData_pt" , 1:4 , ".zip" ) ,
				
					paste0( "https://timss.bc.edu/timss2011/downloads/T11_G4_SPSSData_pt" , 1:3 , ".zip" ) ,
					paste0( "https://timss.bc.edu/timss2011/downloads/T11_G8_SPSSData_pt" , 1:4 , ".zip" ) ,

					paste0( "https://timss.bc.edu/TIMSS2007/PDF/T07_SPSS_G4_" , 1:2 , ".zip" ) ,
					paste0( "https://timss.bc.edu/TIMSS2007/PDF/T07_SPSS_G8_" , 1:2 , ".zip" ) ,
					
					paste0( "https://timss.bc.edu/timss2003i/PDF/t03_spss_" , 1:2 , ".zip" ) ,
					
					paste0( "https://timss.bc.edu/timss1999i/data/bm2_" , c99 , ".zip" ) ,
					
					paste0( "https://timss.bc.edu/timss1995i/database/pop1/POP1_" , c95_1 , ".ZIP" ) ,
					paste0( "https://timss.bc.edu/timss1995i/database/pop2/POP2_" , c95_2 , ".ZIP" ) ,
					paste0( "https://timss.bc.edu/timss1995i/database/pop3/POP3_" , c95_3 , ".ZIP" ) 
				)

			if( catalog[ i , 'year' ] == 1999 ){
				# download the 1999 sas import scripts
				cachaca( 'https://timss.bc.edu/timss1999i/data/bm2_progs.zip' , tf , mode = 'wb' )
				s99 <- unzip( tf , exdir = tempdir() )
			}
			
			if( catalog[ i , 'year' ] == 1995 ){
				# download the 1995 sas import scripts
				cachaca( 'https://timss.bc.edu/timss1995i/database/pop1/POP1PGRM.ZIP' , tf , mode = 'wb' )
				s95_1 <- unzip( tf , exdir = tempdir() )
				cachaca( 'https://timss.bc.edu/timss1995i/database/pop2/POP2PGRM.ZIP' , tf , mode = 'wb' )
				s95_2 <- unzip( tf , exdir = tempdir() )
				cachaca( 'https://timss.bc.edu/timss1995i/database/pop3/POP3PGRM.ZIP' , tf , mode = 'wb' )
				s95_3 <- unzip( tf , exdir = tempdir() )
				s95 <- c( s95_1 , s95_2 , s95_3 )
			}
			
			# for each file to download..
			for ( j in grep( catalog[ i , 'year' ] , ftd , value = TRUE ) ){
				
				# download the damn file
				cachaca( j , tf , mode = 'wb' )
				
				# unzip the damn file
				unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )
				
				# copy all unzipped files into the year-appropriate directory
				stopifnot( all( file.copy( unzipped_files , paste0( catalog[ i , 'output_folder' ] , "/" , tolower( basename( unzipped_files ) ) ) ) ) )
				
				
				
			}


			# # # # # # # # # # #
			# import all files  #
			# # # # # # # # # # #
			
			# construct a vector with all downloaded files
			files <- list.files( catalog[ i , 'output_folder' ] , full.names = TRUE )
			
			# figure out the unique three-character prefixes of each file
			prefixes <- unique( substr( basename( files ) , 1 , 3 ) )

			if( catalog[ i , 'year' ] >= 2003 ){
							
				# loop through each prefix
				for ( p in prefixes ){
				
					# confirm no overwriting
					if( file.exists( paste0( catalog[ i , 'output_folder' ] , '/' , p , '.rds' ) ) ) stop( "rds file already exists. delete your working directory and try again." )

					# initiate an empty object
					y <- NULL
				
					# loop through each saved file matching the prefix pattern
					for ( this.file in files[ substr( basename( files ) , 1 , 3 ) == p ] ){
				
						# read the file into RAM
						x <- haven::read_spss( this.file )
						
						# coerce the file into a data.frame object
						x <- as.data.frame( x , optional = TRUE )
						
						if( nrow( x ) > 0 ){
						
							if ( !is.null( y ) & catalog[ i , 'year' ] == 2003 & any( !( names( x ) %in% names( y ) ) ) ) for ( j in names( x )[ !( names( x ) %in% names( y ) ) ] ) y[ , j ] <- NA
							
							if ( !is.null( y ) & catalog[ i , 'year' ] == 2003 & any( !( names( y ) %in% names( x ) ) ) ) for ( j in names( y )[ !( names( y ) %in% names( x ) ) ] ) x[ , j ] <- NA
					
							# stack it
							y <- rbind( y , x ) ; rm( x ) ; gc()
						
						}
						
						# remove the original file from the disk
						file.remove( this.file )
						
					}
					
					# make all column names lowercase
					names( y ) <- tolower( names( y ) )
					
					# save that single all-country stack-a-mole
					saveRDS( y , file = paste0( catalog[ i , 'output_folder' ] , '/' , p , '.rds' ) , compress = FALSE )
					
				}
				
			} else {
			
				if ( catalog[ i , 'year' ] == 1999 ){
					suf <- c( 'm1' , 'm2' )
					sasf <- s99
					se <- 8
				} else {
					suf <- 1
					sasf <- s95
					se <- 7
				}
				
				# loop through both suffixes
				for ( s in suf ){
							
					# loop through each prefix
					for ( p in prefixes ){
					
						# confirm no overwriting
						if( file.exists( paste0( catalog[ i , 'output_folder' ] , '/' , p , s , '.rds' ) ) ) stop( "rds file already exists. delete your working directory and try again." )

						if( catalog[ i , 'year' ] == 1995 ){
						
							this.sas <- sasf[ grep( toupper( paste0( p , "(.*)\\.sas" ) ) , toupper( basename( sasf ) ) ) ]
						
							if( length( this.sas ) > 1 ) this.sas <- this.sas[ !grepl( "score" , tolower( this.sas ) ) ]
						
						} else {
						
							this.sas <- sasf[ grep( toupper( paste0( p , "(.*)" , s , "\\.sas" ) ) , toupper( basename( sasf ) ) ) ]
					
							if( length( this.sas ) > 1 ) this.sas <- this.sas[ grep( "ctrm" , tolower( this.sas ) ) ]
							
						}
							
						# initiate an empty object
						y <- NULL
					
						# loop through each saved file matching the prefix and suffix pattern
						for ( this.file in files[ substr( basename( files ) , 1 , 3 ) == p & substr( basename( files ) , 7 , se ) == s ] ){
					
							# read the file into RAM
							x <- read_SAScii( this.file , this.sas )
							
							# coerce the file into a data.frame object
							x <- as.data.frame( x , optional = TRUE )
							
							# stack it
							y <- rbind( y , x ) ; rm( x ) ; gc()
							
							file.remove( this.file )
							
						}
						
						if( !is.null( y ) ){
							
							# make all column names lowercase
							names( y ) <- tolower( names( y ) )
							
							# some earlier files have `jkindic` instead of `jkrep`.  fix that.
							if( 'jkindic' %in% names( y ) & !( 'jkrep' %in% names( y ) ) ) names( y ) <- gsub( 'jkindic' , 'jkrep' , names( y ) )
							
							# save that single all-country stack-a-mole
							saveRDS( y , file = paste0( catalog[ i , 'output_folder' ] , '/' , p , s , '.rds' ) , compress = FALSE )
					
						}
					}
				}
			}	

			# the current working directory should now contain one r data file (.rds)
			# for each original prefixed data.frame objects, all separated by year-specific folders.

		
		
		
			for ( rdss in rev( list.files( catalog[ i , 'output_folder' ] , full.names = TRUE ) ) ){
			
				dfx <- readRDS( rdss )
				
				ppv <- grep( "(.*)0[1-5]$" , names( dfx ) , value = TRUE )
				
				ppv <- unique( gsub( "0[1-5]$" , "" , ppv ) )
				
				# actual plausible values
				pv <- NULL
				
				# confirm '01' thru '05' are in the data set.
				for ( j in ppv ) if ( all( paste0( j , '0' , 1:5 ) %in% names( dfx ) ) ) pv <- c( pv , j )
				
				# if there are any plausible values variables,
				# the survey design needs to be both multiply-imputed and replicate-weighted.
				if( length( pv ) > 0 ){
				
					# loop through all five iterations of the plausible value
					for ( j in 1:5 ){
							
						y <- dfx
				
						# loop through each plausible value variable
						for ( vn in pv ){
						
							# copy over the correct iteration into the main variable
							y[ , vn ] <- y[ , paste0( vn , '0' , j ) ]
							
							# erase all five originals
							y <- y[ , !( names( y ) %in% paste0( vn , '0' , 1:5 ) ) ]

						}
						
						# save the implicate
						DBI::dbWriteTable( db , paste( gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , catalog[ i , 'year' ] , j , sep = "_" ) , y )
						
					}
					
				} else {	
				
					DBI::dbWriteTable( db , paste( gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , catalog[ i , 'year' ] , sep = "_" ) , dfx )
				
				}
				
				# make the replicate weights table, make the survey design
				if( 'totwgt' %in% names( dfx ) | 'tchwgt' %in% names( dfx ) ){
					
					if( 'totwgt' %in% names( dfx ) ) wgt <- 'totwgt' else wgt <- 'tchwgt'
				
					z <- dfx[ , c( wgt , 'jkrep' , 'jkzone' ) ]

					rm( dfx ) ; gc()

					for ( j in 1:75 ){
						z[ z$jkzone != j , paste0( 'rw' , j ) ] <- z[ z$jkzone != j , wgt ]
						z[ z$jkzone == j & z$jkrep == 1 , paste0( 'rw' , j ) ] <- z[ z$jkzone == j & z$jkrep == 1 , wgt ] * 2
						z[ z$jkzone == j & z$jkrep == 0 , paste0( 'rw' , j ) ] <- 0
					}

					design_weights <- z[ , paste0( 'rw' , 1:75 ) ]
					
					rm( z )
						
					# where there any imputed variables?
					if( length( pv ) > 0 ){
					
						five_tablenames <- paste( gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , catalog[ i , 'year' ] , 1:5 , sep = "_" )
					
						# if so, construct a multiply-imputed,
						# database-backed, replicate-weighted
						# complex sample survey design.
						design <- 
							survey::svrepdesign( 
								weights = as.formula( paste( "~" , wgt ) )  , 
								repweights = design_weights , 
								data = mitools::imputationList( datasets = as.list( five_tablenames ) , dbtype = "SQLite" ) , 
								type = "other" ,
								combined.weights = TRUE , 
								dbtype = "SQLite" ,
								dbname = catalog[ i , 'dbfile' ]
							)


						# workaround for a bug in survey::svrepdesign.character
						design$mse <- TRUE
											

					} else {
					
						one_tablename <- paste( gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , catalog[ i , 'year' ] , sep = "_" )
					
						# otherwise, construct a
						# database-backed, replicate-weighted
						# complex sample survey design
						# without the multiple imputation.
						design <- 
							survey::svrepdesign( 
								weights = as.formula( paste( "~" , wgt ) )  , 
								repweights = design_weights , 
								data = one_tablename , 
								type = "other" ,
								combined.weights = TRUE ,
								dbtype = "SQLite" ,
								dbname = catalog[ i , 'dbfile' ]
							)
							
						# workaround for a bug in survey::svrepdesign.character
						design$mse <- TRUE

					}

					catalog[ i , 'case_count' ] <- nrow( design )
					
					saveRDS( design , file = paste0( catalog[ i , 'output_folder' ] , '/' , gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , '_design.rds' ) , compress = FALSE )
					
					saveRDS( design_weights , file = paste0( catalog[ i , 'output_folder' ] , '/' , gsub( "(.*)\\.(.*)" , "\\1" , basename( rdss ) ) , '_weights.rds' ) , compress = FALSE )
					
				}
				
			}

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}
