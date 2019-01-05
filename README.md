Manage sparse files in a Windows enviornment. CopySparse and PipeSparse should
work on WinXP and later (both 32/64-bit). MakeSparse requires at least Vista
since it makes use of IO threadpools.

CopySparse and MakeSparse both accept -p as their first parameter to preserve
the file times of the file being copied or modified.

I initially wrote all of these tools to manage my own data files and as such
was focused primarily on getting something that worked well enough rather than
something that looked pretty. As such there is a lot of duplication between the
utilities and they could all use a lot of cleanup. I hope you find these tools
useful, and if you're so inclined, help contribute to making them better.

Happy Hacking!
