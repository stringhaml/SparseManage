Manage sparse files in Windows Vista and later.

MakeSparse can accept -p to preserve the file times of the file being modified.
It also accepts -m to print a sparse cluster map.

CopySparse accept -m to print the sparse cluster map of the copied file if so
desired.

PipeSparse is useful to extract compressed files directly to sparse files.

I initially wrote all of these tools to manage my own data files and as such
was focused primarily on getting something that worked well enough rather than
something that looked pretty. There is a lot of duplication between the
utilities and they could all use a lot of cleanup. I hope you find these tools
useful, and if you're so inclined, help contribute to making them better.

Happy Hacking!
