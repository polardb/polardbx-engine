import os
import readline
# import completion

srcdir = 'Adapter/impl'
blddir = 'Adapter/impl/build'
VERSION = '0.35'

def set_options(opt):
  opt.tool_options('compiler_cxx')
  opt.add_option('--mysql', action='store', default='/usr/local/mysql/')
  opt.add_option('--interactive', action='store_true', dest='interactive')

def configure(conf):
  import Options
  
  if(Options.options.interactive or Options.options.mysql == '-interactive-'):
    # readline config goes here.
    mysql_path = raw_input("mysql location: ")
  else:
    mysql_path = Options.options.mysql
  
  my_lib = mysql_path + "/lib/"
  my_inc = mysql_path + "/include/"

  if os.path.isdir(my_lib + "/mysql"):
    my_lib = my_lib + "/mysql"
 
  if os.path.isfile(my_inc + "mysql/mysql.h"):
    my_inc = my_inc + "mysql/"
  
  ndb_inc = my_inc + "/storage/ndb"
 
  conf.env.my_lib = my_lib
  conf.env.my_inc = my_inc
  conf.env.ndb_inc = ndb_inc
    
  conf.check_tool('compiler_cxx')
  conf.check_tool('node_addon')

  conf.recurse("Adapter/impl/")

def build(ctx):
  ctx.recurse("Adapter/impl/")

