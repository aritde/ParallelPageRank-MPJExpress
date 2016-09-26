# MPJParallelPageRank

This program calculates the page rank of 100 nodes which is provided in an input file.

Input File : pagerank.input.1000.urls.14

Source Code : MPIPageRank.java

Output File : G14_pagerank_output.txt


Before compiling in a Linux system, please make the following config changes :

wget https://sourceforge.net/projects/mpjexpress/files/releases/mpj-v0_38.tar.gz/download 
mv download mpj-v0_38.tar.gz 
tar -zxvf mpj-v0_38.tar.gz
You will get a folder named mpj-v0_42. Open the “.bashrc” file:
vim ~/.bashrc

Then type the following commands at the top of the “.bashrc” file.
export MPJ_HOME=/path-to-mpj-unzipped-folder/ 
export PATH=$PATH:$MPJ_HOME/bin

Then type the below in root:
source ~/.bashrc


Compilation procedure : javac -cp .:$MPJ_HOME/lib/mpj.jar MPIPageRank.java

Execution Command : mpjrun.sh -np 4 MPIPageRank pagerank.input.1000.urls.14 G14_pagerank_output.txt 0.85 400

Notations : mpjrun.sh -np 4 MPIPageRank <input file name> <output file name> <damping factor> <no of iterations>




