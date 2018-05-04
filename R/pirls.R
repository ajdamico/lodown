get_catalog_pirls <-
	function( data_name = "pirls" , output_dir , ... ){

		catalog <-
			data.frame(
				year = seq( 2001 , 2016 , 5 ) ,
				stringsAsFactors = FALSE
			)

		catalog$output_folder <- paste0( output_dir , "/" , catalog$year , "/" )
		
		catalog

	}


lodown_pirls <-
	function( data_name = "pirls" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

		
			# # # # # # # # # # # #
			# download all files  #
			# # # # # # # # # # # #

			# specify the pathway to each and every spss data set to download.
			ftd <-
				c(
					"https://timssandpirls.bc.edu/pirls2016/international-database/downloads/P16_SPSSData_pt1.zip" ,
					"https://timssandpirls.bc.edu/pirls2016/international-database/downloads/P16_SPSSData_pt2.zip" ,
					"https://timssandpirls.bc.edu/pirls2011/downloads/P11_SPSSData_pt1.zip" ,
					"https://timssandpirls.bc.edu/pirls2011/downloads/P11_SPSSData_pt2.zip" ,
					"https://timssandpirls.bc.edu/PDF/PIRLS2006_SPSSData.zip" ,
					"https://timssandpirls.bc.edu/pirls2001i/Pirls2001Database/pirls_2001_spssdata.zip"
				)
				
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

			# loop through each prefix
			for ( p in prefixes ){
			
				# initiate an empty object
				y <- NULL
				
				# loop through each saved file matching the prefix pattern
				for ( this.file in files[ substr( basename( files ) , 1 , 3 ) == p ] ){
				
					# read the file into RAM
					x <- haven::read_spss( this.file )
					
					# coerce the file into a data.frame object
					x <- as.data.frame( x )
					
					# stack it
					y <- rbind( y , x ) ; rm( x ) ; gc()
					
					# remove the original file from the disk
					file.remove( this.file )
					
				}
				
				# make all column names lowercase
				names( y ) <- tolower( names( y ) )

				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( y ) , na.rm = TRUE )
				
				# save that single all-country stack-a-mole
				saveRDS( y , file = paste0( catalog[ i , 'output_folder' ] , '/' , p , '.rds' ) , compress = FALSE ) ; rm( y ) ; gc()
				
			}


			# here are the combinations to create #
	
			# asa alone #
			asa <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/asa.rds' ) )
			asa_design <- pirls_pvsd( asa , 'totwgt' , pirls_rwcf( asa , 'totwgt' ) ) ; rm( asa ) ; gc()
			saveRDS( asa_design , file = paste0( catalog[ i , 'output_folder' ] , '/asa_design.rds' ) , compress = FALSE ) ; rm( asa_design ) ; gc()
			
			# asg alone #
			asg <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/asg.rds' ) )
			asg_design <- pirls_pvsd( asg , 'totwgt' , pirls_rwcf( asg , 'totwgt' ) )
			saveRDS( asg_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_design.rds' ) , compress = FALSE ) ; rm( asg_design ) ; gc()
			
			# asg + ash
			ash <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/ash.rds' ) )
			# note: these two files are too big to merge in a smaller computer's RAM
			# so only keep the weighting/jackknife variables from the `asg` table for this iteration.
			asg_ash <- merge( asg[ , c( 'idcntry' , 'idstud' , 'totwgt' , 'jkzone' , 'jkrep' ) ] , ash , by = c( 'idcntry' , 'idstud' ) )
			stopifnot( nrow( asg_ash ) == nrow( ash ) & nrow( ash ) == nrow( asg ) )

			asg_ash_design <- pirls_pvsd( asg_ash , 'totwgt' , pirls_rwcf( asg_ash , 'totwgt' ) ) ; rm( asg_ash ) ; gc()
			saveRDS( asg_ash_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_ash_design.rds' ) , compress = FALSE ) ; rm( asg_ash_design ) ; gc()

			
			# asg + acg
			acg <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/acg.rds' ) )
			asg_acg <- merge( asg , acg , by = c( 'idcntry' , 'idschool' ) ) ; rm( acg ) ; gc()
			stopifnot( nrow( asg_acg ) == nrow( asg ) ) ; rm( asg ) ; gc()
			
			asg_acg_design <- pirls_pvsd( asg_acg , 'totwgt' , pirls_rwcf( asg_acg , 'totwgt' ) ) ; rm( asg_acg ) ; gc()
			saveRDS( asg_acg_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_acg_design.rds' ) , compress = FALSE ) ; rm( asg_acg_design ) ; gc()

			
			# ast alone #
			ast <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/ast.rds' ) )
			ast_design <- pirls_pvsd( ast , 'tchwgt' , pirls_rwcf( ast , 'tchwgt' ) )
			saveRDS( ast_design , file = paste0( catalog[ i , 'output_folder' ] , '/ast_design.rds' ) , compress = FALSE ) ; rm( ast_design ) ; gc()

			
			# ast + atg
			atg <- readRDS( paste0( catalog[ i , 'output_folder' ] , '/atg.rds' ) )
			ast_atg <- merge( ast , atg , by = c( 'idcntry' , 'idteach' , 'idlink' ) ) ; rm( atg ) ; gc()
			stopifnot( nrow( ast_atg ) == nrow( ast ) )

			ast_atg_design <- pirls_pvsd( ast_atg , 'tchwgt' , pirls_rwcf( ast_atg , 'tchwgt' ) ) ; rm( ast_atg ) ; gc()
			saveRDS( ast_atg_design , file = paste0( catalog[ i , 'output_folder' ] , '/ast_atg_design.rds' ) , compress = FALSE ) ; rm( ast_atg_design ) ; gc()
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}



# initiate a replicate weights creation function to quickly compute weights wherever necessary
pirls_rwcf <- 
	function( z , wgt ){
		for ( i in 1:75 ){
			
			z[ z$jkzone != i , paste0( 'rw' , i ) ] <- z[ z$jkzone != i , wgt ]
			
			z[ z$jkzone == i & z$jkrep == 1 , paste0( 'rw' , i ) ] <- z[ z$jkzone == i & z$jkrep == 1 , wgt ] * 2
			z[ z$jkzone == i & z$jkrep == 0 , paste0( 'rw' , i ) ] <- 0
			
		}
		z[ , paste0( 'rw' , 1:75 ) ]
}


# initiate a plausible valued survey design object creation function
pirls_pvsd <-
	function( x , wgt , rw ){
	
		# search for plausible values variables.
		# plausible values variables are named _varname_01 thru _varname_05
		ppv <- grep( "(.*)0[1-5]$" , names( x ) , value = TRUE )
		
		ppv <- unique( gsub( "0[1-5]$" , "" , ppv ) )
		
		# actual plausible values
		pv <- NULL
		
		# confirm '01' thru '05' are in the data set.
		for ( i in ppv ) if ( all( paste0( i , '0' , 1:5 ) %in% names( x ) ) ) pv <- c( pv , i )
		
		
		# if there are any plausible values variables,
		# the survey design needs to be both multiply-imputed and replicate-weighted.
		if( length( pv ) > 0 ){
		
			# loop through all five iterations of the plausible value
			for ( i in 1:5 ){
					
				y <- x
		
				# loop through each plausible value variable
				for ( vn in pv ){
				
					# copy over the correct iteration into the main variable
					y[ , vn ] <- y[ , paste0( vn , '0' , i ) ]
					
					# erase all five originals
					y <- y[ , !( names( y ) %in% paste0( vn , '0' , 1:5 ) ) ]

				}
				
				# save the implicate
				assign( paste0( 'x' , i ) , y ) ; rm( y ) ; gc()
			}
			
			rm( x ) ; gc()
			
			z <-
				survey::svrepdesign( 	
					weights = as.formula( paste( "~" , wgt ) ) , 
					repweights = rw ,
					rscales = rep( 1 , 75 ) ,
					scale = 1 ,
					type = 'other' ,
					data = mitools::imputationList( mget( paste0( "x" , 1:5 ) ) ) ,
					mse = TRUE
				)
				
			rm( x1 , x2 , x3 , x4 , x5 ) ; gc()
				
		# otherwise, it's simply replicate-weighted.
		} else {
		
			z <-
				survey::svrepdesign( 	
					weights = as.formula( paste( "~" , wgt ) ) , 
					repweights = rw ,
					rscales = rep( 1 , 75 ) ,
					scale = 1 ,
					type = 'other' ,
					data = x ,
					mse = TRUE
				)
				
			rm( x ) ; gc()
				
		}
	
		z
	}



