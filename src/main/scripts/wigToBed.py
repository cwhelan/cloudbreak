import sys

print "track name=\"positive peaks\""

sys.stdin.readline()
sys.stdin.readline()

in_pos_peak = False
max_score_in_peak = 0
sum_peak_scores = 0
num_peak_pos = 0
peak_start = 0
peak_num = 1

for line in sys.stdin:
    fields = line.split("\t")    
    pos = int(fields[0])
    val = float(fields[1])
    if val > 0:
        if in_pos_peak:
            if val > max_score_in_peak:
                max_score_in_peak = val
            sum_peak_scores = sum_peak_scores + val            
        else:
            peak_start = pos
            in_pos_peak = True
            max_score_in_peak = val
            sum_peak_scores = val
    else:
        if in_pos_peak:
            end_pos = pos + 49   
#            print "\t".join(["chr2",str(peak_start),str(end_pos),str(peak_num),str(max_score_in_peak)])
            print "\t".join(["chr2",str(peak_start),str(end_pos),str(peak_num),str(sum_peak_scores)])
            peak_num = peak_num + 1
            in_pos_peak = False        
                           
            
