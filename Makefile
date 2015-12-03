all:
	go test gopkg.in/pg.v4 -cpu=1,2,4
	go test gopkg.in/pg.v4 -short -race
	go test gopkg.in/pg.v4/orm -cpu=1,2,4
	go test gopkg.in/pg.v4/orm -short -race
