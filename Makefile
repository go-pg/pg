all:
	go test gopkg.in/pg.v3 -cpu=1,2,4
	go test gopkg.in/pg.v3 -short -race
