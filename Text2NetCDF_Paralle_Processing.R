library(ncdf4)
library(furrr)
library(dplyr)

xvals <- seq(68.375, 97.125, 0.25)
yvals <- seq(8.125, 36.875, 0.25) 
nx <- length(xvals)
ny <- length(yvals)
lon1 <- ncdim_def("longitude", "degrees_east", xvals)
lat2 <- ncdim_def("latitude", "degrees_north", yvals)

createncfile <- function(foldername){
	spltname <- unlist(strsplit(foldername, ','))

	outfilename = spltname[2]
	scrfolder = spltname[1]
		
	filelist = list.files(scrfolder,'*.txt',full.names=T)
	initdat <- read.delim(filelist[1],header=T,stringsAsFactors=F)
	nodays <- length(initdat[,1])

	emptydata<-array(-999,c(nx,ny,nodays))
	for(flname in filelist){

		latlon <- unlist(strsplit(sub('.txt','',basename(flname)),'_'))

		lon <- as.numeric(paste0(latlon[1],'5'))/1000
		lat <- as.numeric(paste0(latlon[2],'5'))/1000

		lonloc<-which(xvals==lon)
		latloc<-which(yvals==lat)

		data<-read.delim(flname,header=T,stringsAsFactors=F)
		emptydata[lonloc,latloc,] <- data[,1]/86400
	}

	time <- ncdim_def("time", "days since 1951-01-01 00:00:00", 0:(nodays-1), unlim=TRUE)
	mv <- -999 #missing value to use
	var_temp <- ncvar_def("pr", "kg m-2 s-1", list(lon1, lat2, time), longname="Precipitation", mv) 
	##var_temp <- ncvar_def(var name, unit, dim, longname, missing value)

	#print(Sys.time())
	ncnew <- nc_create(outfilename, list(var_temp))

	ncvar_put(ncnew, var_temp, emptydata)

	# Don't forget to close the file
	nc_close(ncnew)
	print(Sys.time())
	
	rm(emptydata,outfilename,scrfolder,filelist,initdat,nodays,latlon,lon,lat,lonloc,latloc,data,time,mv,var_temp,ncnew)
	gc()
	
}

# file_names <- list.files(path = "example_data/",full.names = T)
folder_names <- read.csv("/AR6/India/Path_for_Tmax_nc.csv",header=T,sep='',stringsAsFactors=F)
folder_names <- unlist(split(folder_names, rownames(folder_names)))
folder_names <- as.character(folder_names)

set.seed(123)
plan(multisession,workers = availableCores())
folder_names %>% future_map(createncfile,.options = furrr_options(seed = TRUE))

plan(sequential)
