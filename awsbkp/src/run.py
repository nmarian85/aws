import sys
import subprocess as sp
import hashlib


def run_cmd(cmd, logger):
	try:
		p = sp.Popen(cmd, stdout = sp.PIPE, stderr = sp.PIPE)
		out, err = p.communicate()
		if err:
			 logger.error("Error: ", err.strip())
	except OSError as e:
		 logger.error("Error: " + e.strerror)
	except:
		 logger.error("Error: ", sys.exc_info()[0])
	# p_status = p.wait()
	# print("Command output : " + output
	# print("Command exit status/return code : ", p_status
	return out.decode("ISO-8859-1"), p.returncode

def get_dirs(src_path, s_d, r_d, logger):
	out, ret = run_cmd(["find", src_path, "-mindepth", "1", "-maxdepth", "1", "-type", "d", "-newermt", s_d, "!", "-newermt", r_d], logger)
	return out, ret

def get_files(src_path, s_d, r_d, logger):
	out, ret = run_cmd(["find", src_path, "-mindepth", "1", "-maxdepth", "1", "-type", "f", "-newermt", s_d, "!", "-newermt", r_d], logger)
	return out, ret

def calc_fmd5(f):
	with open(f, "rb") as binary_file:
		data = binary_file.read()
	md5 = hashlib.md5(data).hexdigest()
	# logger.info("md5sum of " + f + " is " + str(md5))
	return md5
