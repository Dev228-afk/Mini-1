# Observations of data

version 0.1 

Focus: Threading and memory observations and relationships

Welcome to our first research project (mini1, 1 of 3). We are
focusing on how parallelization and data structures interact. 
The goal of the project is not a functional (app-based) driven
outcome, rather to observe how design affects memory and the 
use of resources. 

To start all work is performed within a single process to allow
better utilization of tools and make exploring (and debugging)
easier as there are less process-to-process complexities.


## Resources

There are several resources uploaded in Canvas to support this 
investigation. They are:

   * Data sets. Two different set have been uploaded for use in
     assessing your design and investigations.

     ** 2020 Fire data
     ** Population data (worldbank)

   * Code. Using the **hello lab's** code as an example, 
     building (Cmake) and threading (for-loop, C++/OMP), you are to
     code a data retrieval code using the two data sets, creating 
     a C++ library interface for each. 

     Objectives are to code well, but there are more important 
     considerations, the design, and observations on alternative
     designs.
 

## Approach (Steps 1, 2, 3, and more)

   1. Design and code (C++) heavily towards what you believe 
      is the best approach for storing and retrieving data. Choosing
      an Object Oriented Design, placing emphasise on long-term use is
      important. 

      e.g., your code will be used in a larger code base so, designing
      the appropriate proxy/facade is key to longivity. Suggestions like
      first class objects, virtual classes, templates, and patterns are
      encouraged. 
 
      Your code should provide data encapsulation, an API for extraction, 
      and basic range searching to support your theories.
 
      **Do not use threads for this part we are serial processing.**
 
      *Hint: Data and interface abstraction (facade pattern).*
      *Hint: What kind of testing harness will you need?*
      *Hint: Look at parser.tar.gz for tool suggestions*
      *Hint: What types of searches do you expect?*

   2. Benchmark your code against the two data sets. Note think
      about what would give you the most representitive average of
      your codes' performance. 

      This is an important step as you will want to determine if
      parallel efforts make a difference.

   3. Optimize/Parallelize (threading) and modify your design.
 
      Knowing OMP or pthreads. What are your design decisions?

      *Be prepared to defend your choices!*

   4. Rerun your benchmarking. 

      Consider both successes and failures in your report as
      failures teach us as much as gains. Lastly, remember to 
      save results for your report.
 
      *Hint: What could affect testing results?*
      *Hint: Do the expected queries shape your design?*
      *Hint: This is where your code harness helps.*
      *Hint: Do the expected queries shape your design?*


### Language and Tools

The following is a list of languages and tools suggested to complete 
the project:

   Languages and Libraries
   * C/C++ (both)
     * gcc/g++, v13 or newer
     * Clang (not apple's xcode) v16 or newer
   * Third-party libraries - none.

   Tools
   * cmake (https://cmake.org)
   * Linters for C/C++ 
   * lmod (https://github.com/TACC/Lmod) - optional
   * Editors (2+ recommended): neovim/vim, vscode, IDEs, etc.
   * Terminal: kitty, iterm2, OS native, etc
        ** You may want/need a font (https://www.nerdfonts.com) or two
        ** A shell scripting language (e.g., bash, tcsh,)
              *** https://www.freecodecamp.org/news/bash-scripting-tutorial-linux-shell-script-and-command-line-for-beginners/
              *** https://www.tutorialspoint.com/unix/shell_scripting.htm 
   * Optional: Valgrind suite, perf, or other like tools


### Restrictions and Limits

As this lab has been constructed with a specific set of learning and
discovery objectives, some aspects are artifically constrained. Yes,
many can be engineered out and exist in the wild yet, yet, the concepts 
remain valid. Therefore, there are several creativity boundaries that 
need to be put in place:

  * No third party libraries or services other than what has been 
    listed above.
  * Do not circumvent the spirit of the mini's goals/instructions.
  * AI likely is not going to help you code, it will make it look
    professional, but looks can be limited. More importantly, is
    it constructed and reasoned as a professional?


### What to turn in

A `tar.gz` file containing a folder with 1) code, 2) report, and 
3) presentation. E.g., teamducks.tar.gz. Also, one submission
per team to canvas. You can (re)submit multiple times before the
due date. 

**The due date is before the class presentations. No exceptions.**

**Please do not include test data.**

The presention is **exactly one slide** (imagine a poster) to be
presented to the class. The slide should be focusing on a single
important discovery your team made or a key experience you want
to pass to others. 

*Hint: It (slide) should not be: A project summary, a diagram of your 
classes, a tutorial, and most definitely not vague.*

The report is the supporting facts and approach leading to conclusion
and presentation. Please include a discussion on how you achieved your 
solution, citations, and individual contributions.  This is your team's 
justification with data (tabular and graph formats) and observations 
backing up your conclusions and recommendations.  Be sure to include 
attempts irregardless whether they helped. 

*Hint: Supporting your conclusions largely affects your points.*


### What about AI?

Well, this has been the elephant in the room for many. So, how 
do we proceed. This will be interesting. If you choose to use 
AI to help, know this.

   * Your skills to use AI verses knowing how to 
     design/research/defend, and code is the tension that you 
     must balance. Interviewers are most interested in your 
     knowledge and reasoning skills.
   * Demark in code the start/end of AI usage, failure to do 
     nulls the assignment.


### Report

The reports is the capstone of your observations and must include
citations used in your investigations. When presenting your data
in the report please use both tablular and graphs. For graphs,
consider what type of graph is appropriate and the choice of the
scale of the y-axis (range vs log). 

Please do not make the report a summary of your design. Tell me 
about your discoveries, reasoning, and observations!
