cur_branch := $(shell cd ..; git rev-parse --abbrev-ref HEAD)
obj_dir := ../obj.$(cur_branch)

protobufs: RCDB.proto
	protoc --cpp_out=. RCDB.proto
	protoc --python_out=. RCDB.proto
	g++ -std=c++0x -c -o RCDB.pb.o RCDB.pb.cc

TwitterGraphBatchLoader: protobufs TwitterGraphBatchLoaderMain.cc
	g++ -g -O3 -fno-strict-aliasing -MD -msse4.2 -DNDEBUG -Wno-unused-variable -march=core2 -DINFINIBAND -std=c++0x -I../src -I../obj.master -I../gtest/include -I/usr/local/openonload-201405/src/include -Werror -Wall -Wformat=2 -Wextra -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute -Wno-non-template-friend -Woverloaded-virtual -Wcast-qual -Wcast-align -Wconversion -Weffc++ -fPIC -c -o TwitterGraphBatchLoaderMain.o TwitterGraphBatchLoaderMain.cc
	g++ -o TwitterGraphBatchLoader TwitterGraphBatchLoaderMain.o RCDB.pb.o ../obj.master/OptionParser.o ../obj.master/libramcloud.a -L../obj.master  /usr/local/lib/libzookeeper_mt.a -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -libverbs

TwitterWorkloadClient: protobufs TwitterWorkloadClientMain.cc
	g++ -g -O3 -fno-strict-aliasing -MD -msse4.2 -DNDEBUG -Wno-unused-variable -march=core2 -DINFINIBAND -std=c++0x -I../src -I../obj.master -I../gtest/include -I/usr/local/openonload-201405/src/include -Werror -Wall -Wformat=2 -Wextra -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute -Wno-non-template-friend -Woverloaded-virtual -Wcast-qual -Wcast-align -Wconversion -Weffc++ -fPIC -c -o TwitterWorkloadClientMain.o TwitterWorkloadClientMain.cc
	g++ -o TwitterWorkloadClient TwitterWorkloadClientMain.o RCDB.pb.o ../obj.master/OptionParser.o ../obj.master/libramcloud.a -L../obj.master  /usr/local/lib/libzookeeper_mt.a -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -libverbs	

all: protobufs TwitterGraphBatchLoader TwitterWorkloadClient
