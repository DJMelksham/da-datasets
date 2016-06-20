(defun make-csv (source
		 &key
		   (delimiter *delimiter*)
		   (quote-char *quote-char*)
		   (newline-char *newline-char*)
                   (read-buffer-size *read-buffer-size*)
		   (col-names nil)
		   (first-row-col-names-p t)
		   (num-rows nil)
		   (num-cols nil)
		   (rectangular nil))
"Make a delimited object (typically a csv file, but can also accept a stream)"
  (let ((file-path nil)
	(delimiter-stream nil)
	(file-size nil)
	(real-read-buffer-size nil)
	(real-col-names nil)
	(real-num-rows nil)
	(real-num-cols nil)
	(real-rectangular nil))

    ;; Assert delimiter
    (assert (characterp delimiter))
    
    ;; Assert quote-char
    (assert (characterp quote-char))
    
    ;; Assert newline-char
    (assert (characterp newline-char))
    
    ;; Determine whether source is file-path or stream
    ;; Set file-path and/or stream respectively

    (assert (or (stringp source)
		(pathnamep source)
		(streamp source))) 
    
    (cond ((stringp source) (setf file-path (uiop:parse-unix-namestring source)))
	  ((pathnamep source) (setf file-path source))
	  ((streamp source) (setf delimiter-stream source))
	  (t nil))

    (if (and (null delimiter-stream)
	     (not (probe-file file-path)))
	(error "Either source was not a stream, or the file designated can't be found"))
		   
    ;; Set file size (assuming it is a file)
    (if (null delimiter-stream)
	(with-open-file (stream file-path
				:direction :input)
	  (setf file-size (file-length stream))))
			
    ;; Set buffer size to min of file-size or buffer-size
    (setf real-read-buffer-size (min file-size read-buffer-size))

    ;; 
    
		   
		   (make-instance 'delimited-object
				  :delimiter delimiter
				  :quote-char quote-char
				  :newline-char newline-char
				  :read-buffer-size real-read-buffer-size)))
				  
(defun file-string (path)
  (with-open-file (stream path)
    (let ((data (make-string (file-length stream))))
      (read-sequence data stream)
      data)))


(defun collect-stats (csv-object)
  "Populate statistics for a csv object"
  ())

(defun csv-analysis (stream &key
			      (delimiter *delimiter*)
			      (quote-char *quote-char*)
			      (newline-char *newline-char*)
			      (first-row-col-names-p t)
			      (num-rows t)
			      (num-cols t)
			      (rectangular-p t)
			      (min-buffer-size t)
			      (buffer-size *read-buffer-size*)
			      (rows-per-buffer (* *cpu-count* *csv-rows-per-core*)))
  (let* ((col-names nil)
	 (file-size (file-length stream))
	 (nrows 0)
	 (ncols 0)
	 (min-ncols 0)
	 (max-field-buff-length 0)
	 (max-row-length 0)
	 (rectangular-p nil)
	 (min-buffer-size buffer-size)
	 (read-buff (make-array buffer-size :element-type 'character))
	 (char-buff (make-array (* buffer-size 3) :element-type 'character))
	 (field-buff (make-array (* buffer-size 3) :element-type 'fixnum))
	 (row-buff (make-array (* buffer-size 3) :element-type 'fixnum))
	 (row-holder (make-array (* buffer-size 3) :initial-element #() :element-type 'array))
	 (char-buff-index 0)
	 (field-buff-index 1)
	 (row-buff-index 1)
	 (read-sequence-return 0)
	 (csv-state 0))
    (declare
     (fixnum file-size char-buff-index field-buff-index row-buff-index
	     read-sequence-return csv-state)
     (type (simple-array character) read-buff char-buff)
     (type (simple-array fixnum) field-buff row-buff)
     (optimize (speed 3) (safety 1)))
    ;;; The big analysis section is actually rather complex.  Hold on to your hats...
    (loop while (not (eql (the fixnum (file-position stream)) file-size)) ;loop until we've read the whole file
       ;; First, check for overflow from last round and insert any values from there into the
       ;; beginning of the char-array.  Set other index values to take any such
       ;; insertions into account.
       ;; the read-sequence function should only read and append
	
       do (if (overflow? row-buff char-buff-index row-buff-index) 
	      (multiple-value-setq (char-buff-index
				    field-buff-index
				    row-buff-index)
		(overflow-handover char-buff char-buff-index
				   field-buff field-buff-index
				   row-buff row-buff-index))
	      (setf char-buff-index 0
		    field-buff-index 1
		    row-buff-index 1))
       	 
	 
       ;; We read in the char sequence from the file.  We're using char-buff-index
       ;; as the starting point because this will 'reasonably' reliably
       ;; allow us to compensate for remaining characters brought in from the previous
       ;; iteration.  We can harden it later.
       do (setf read-sequence-return (read-sequence read-buff stream))
       do (loop for char across read-buff
	     do (multiple-value-setq (csv-state
				     char-buff-index
				     field-buff-index
				     row-buff-index)
		 (csv-buffer-manipulator csv-state
					 char
					 delimiter
					 quote-char
					 newline-char
					 char-buff
					 field-buff
					 row-buff
					 char-buff-index
					 field-buff-index
					 row-buff-index)))


;; Extend the buffers if the size of them look like they might prove to be insufficient
       do (if (insufficient-buffer-size? rows-per-buffer row-buff-index)
	      (multiple-value-setq (read-buff
				    char-buff
				    field-buff
				    row-buff
				    row-holder)
		(increase-buffers read-buff
				  char-buff
				  field-buff
				  row-buff
				  row-holder)))
       )

    
  (values (file-position stream) file-size csv-state)))

(defun read-float (array)
	       "Assumes [0-9]*.[0-9]*/[^0-9]"
	       (let ((int-part1 0)
		     (int-part2 0)
		     (i 0)
		     (k 0)
		     (dec 0)
		     (len (length array)))
		 (declare (optimize (speed 3))
			  (fixnum int-part1 int-part2 i k dec len)
			  (type (simple-array character) array))
		 (loop
		    for ch = (aref array i)
		    until (char= #\. ch)
		    do (setf int-part1 (+ (the fixnum (* 10 int-part1))
					  (the fixnum (digit-char-p ch))))
		    do (incf i))
		 (incf i)
		 (setf k (- len (+ i 1)))
		 (setf dec (+ k 1))
		 (loop
		    until (>= i len)  
		    for digit = (the fixnum (digit-char-p (aref array i)))
		    for order = (the fixnum (expt 10 k))
		    do (incf int-part2 (the fixnum (the fixnum (* digit order))))
		    do (decf k)
		    do (incf i))
		 (+ (float int-part1) (float (/ int-part2 (expt 10 dec))))))
