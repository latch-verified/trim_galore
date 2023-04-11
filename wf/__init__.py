from enum import Enum
from pathlib import Path
import subprocess
from textwrap import dedent
from typing import Optional

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import (
    LatchAuthor,
    LatchDir,
    LatchFile,
    LatchMetadata,
    LatchParameter,
    Section,
    Text,
    Params,
)

"""
define workflow metadata
"""
metadata = LatchMetadata(
    display_name="Trim Galore",
    documentation="https://github.com/latch-verified/trim_galore",
    author=LatchAuthor(
        name="Jacob L Steenwyk",
        email="jacob@latch.bio",
        github="https://github.com/JLSteenwyk",
    ),
    repository="https://github.com/latch-verified/trim_galore",
    license="MIT",
    parameters={
        "input_forward": LatchParameter(
            display_name="Input forward",
            description="forward input file",
            hidden=False,
            batch_table_column=True,
        ),
        "input_reverse": LatchParameter(
            display_name="Input reverse",
            description="reverse input file",
            hidden=False,
            batch_table_column=True,
        ),
        "base_out": LatchParameter(
            display_name="Basename for output files",
            description="Default is to derive the basename from the input files",
            hidden=False,
            batch_table_column=True,
        ),
        "output_directory": LatchParameter(
            display_name="Output directory name",
            description="Output directory name",
            hidden=False,
            batch_table_column=True,
        ),
        "base_quality_encoding": LatchParameter(
            display_name="Base Quality Encoding",
            description="Output directory name",
            hidden=False,
        ),
        "fastqc": LatchParameter(
            display_name="Enable FastQC",
            description="Turn on to include FastQC results",
            hidden=False,
            batch_table_column=True,
        ),
        "fastqc_args": LatchParameter(
            display_name="Additional FastQC argument",
            description="add FastQC arguments as a string",
            hidden=False,
        ),
        "adapter_sequence": LatchParameter(
            display_name="Adapter sequence to be trimmed from precurated set",
            description="Adapter sequence to be trimmed. Options are auto-detect, Illumina, stranded_illumina, nextera, and small RNA.",
            hidden=False,
            batch_table_column=True,
        ),
        "adapter": LatchParameter(
            display_name="Adapter sequence to be trimmed",
            description="Adapter sequence to be trimmed. If not specified, Trim Galore will try to auto-detect.",
            hidden=False,
        ),
        "adapter2": LatchParameter(
            display_name="Adapter sequence to be trimmed for read 2 of paired-end files",
            description="Adapter sequence to be trimmed.",
            hidden=False,
        ),
        "consider_already_trimmed": LatchParameter(
            display_name="Consider already trimmed",
            description="Set a threshold up to which the file is considered already adapter-trimmed.",
            hidden=False,
        ),
        "max_length": LatchParameter(
            display_name="Max length",
            description="Discard reads that are longer than <INT> bp after trimming.",
            hidden=False,
        ),
        "max_n": LatchParameter(
            display_name="Max Ns in a read",
            description="The total number of Ns a read may contain before it will be removed altogether.",
            hidden=False,
        ),
        "quality": LatchParameter(
            display_name="Quality threshold",
            description="Trim low-quality ends from reads in addition to adapter removal.",
            hidden=False,
        ),
        "stringency": LatchParameter(
            display_name="Stringency",
            description="Overlap with adapter sequence required to trim a sequence.",
            hidden=False,
        ),
        "error_rate": LatchParameter(
            display_name="Error Rate",
            description="Maximum allowed error rate.",
            hidden=False,
        ),
        "gzip_output_files": LatchParameter(
            display_name="GZip output",
            description="GZip output files.",
            hidden=False,
        ),
        "length": LatchParameter(
            display_name="Read length cutoff",
            description="Discard reads that became shorter than length <INT> because of either quality or adapter trimming.",
            hidden=False,
        ),
        "trim_n": LatchParameter(
            display_name="Trim Ns at ends",
            description="Removes Ns from either side of the read.",
            hidden=False,
        ),
        "report_file": LatchParameter(
            display_name="Generate report files",
            description="Generate report files by default.",
            hidden=False,
        ),
        "clip_R1": LatchParameter(
            display_name="Trim 5' end of read 1",
            description="Instructs Trim Galore to remove <INT> bp from the 5' end of read 1.",
            hidden=False,
        ),
        "clip_R2": LatchParameter(
            display_name="Trim 5' end of read 2",
            description="Instructs Trim Galore to remove <INT> bp from the 5' end of read 2.",
            hidden=False,
        ),
        "three_prime_clip_R1": LatchParameter(
            display_name="Trim 3' end of read 1",
            description="Instructs Trim Galore to remove <INT> bp from the 3' end of read 1.",
            hidden=False,
        ),
        "three_prime_clip_R2": LatchParameter(
            display_name="Trim 3' end of read 2",
            description="Instructs Trim Galore to remove <INT> bp from the 3' end of read 2.",
            hidden=False,
        ),
        "hardtrim5": LatchParameter(
            display_name="Hard-trim 5'-end",
            description="hard-trim sequences to <int> bp at the 5'-end",
            hidden=False,
        ),
        "hardtrim3": LatchParameter(
            display_name="Hard-trim 3'-end",
            description="hard-trim sequences to <int> bp at the 3'-end",
            hidden=False,
        ),
        "polyA": LatchParameter(
            display_name="Remove PolyA tails",
            description="Trim Galore attempts to identify from the first supplied sample whether sequences contain more often a stretch of either 'AAAAAAAAAA' or 'TTTTTTTTTT'",
            hidden=False,
        ),
        "implicon": LatchParameter(
            display_name="Implicon",
            description="For paired-end data, transfer a unique molecular identifier from the start of Read 2 to the readID of both reads",
            hidden=False,
        ),
        "retain_unpaired": LatchParameter(
            display_name="Retain unpaired reads",
            description="If only one of the two paired-end reads became too short, the longer read will be written to either '.unpaired_1.fq' or '.unpaired_2.fq' output files",
            hidden=False,
        ),
        "length_1": LatchParameter(
            display_name="Unpaired single-end read 1 length cutoff",
            description="Unpaired single-end read length cutoff needed for read 1 to be written to '.unpaired_1.fq' output file",
            hidden=False,
        ),
        "length_2": LatchParameter(
            display_name="Unpaired single-end read 2 length cutoff",
            description="Unpaired single-end read length cutoff needed for read 2 to be written to '.unpaired_2.fq' output file",
            hidden=False,
        ),
    },
    flow=[
        Section(
            "Input/Output",
            Params(
                "input_forward",
                "input_reverse",
                "base_out",
                "output_directory",
                "gzip_output_files",
                "retain_unpaired",
            ),
        ),
        Section(
            "Quality",
            Params(
                "base_quality_encoding",
                "quality",
                "length",
                "length_1",
                "length_2",
                "max_n",
                "trim_n",
                "max_length",
            ),
        ),
        Section(
            "Adapters",
            Params(
                "adapter_sequence",
                "error_rate",
                "stringency",
                "adapter",
                "adapter2",
                "consider_already_trimmed",
            ),
        ),
        Section(
            "Reporting",
            Params("fastqc", "fastqc_args", "report_file"),
        ),
        Section(
            "Miscellaneous",
            Params(
                "hardtrim5",
                "hardtrim3",
                "clip_R1",
                "clip_R2",
                "three_prime_clip_R1",
                "three_prime_clip_R2",
                "polyA",
                "implicon",
            ),
        ),
    ],
)


class BaseQualityEncoding(Enum):
    phred33 = "--phred33"
    phred64 = "--phred64"


class AdapterSequence(Enum):
    auto = "auto"
    illumina = "--illumina"
    stranded_illumina = "--stranded_illumina"
    nextera = "--nextera"
    small_rna = "--small_rna"


def _fmt_dir(bucket_path: str) -> str:
    if bucket_path[-1] == "/":
        return bucket_path[:-1]
    return bucket_path


@small_task
def trim_reads_task(
    input_forward: LatchFile,
    input_reverse: LatchFile,
    base_out: Optional[str],
    output_directory: Optional[LatchDir],
    fastqc_args: Optional[str],
    adapter: Optional[str],
    adapter2: Optional[str],
    consider_already_trimmed: Optional[int],
    max_length: Optional[int],
    max_n: Optional[float],
    clip_R1: Optional[int],
    clip_R2: Optional[int],
    three_prime_clip_R1: Optional[int],
    three_prime_clip_R2: Optional[int],
    hardtrim5: Optional[int],
    hardtrim3: Optional[int],
    quality: int = 20,
    base_quality_encoding: BaseQualityEncoding = BaseQualityEncoding.phred33,
    fastqc: bool = True,
    adapter_sequence: AdapterSequence = AdapterSequence.auto,
    stringency: int = 1,
    error_rate: float = 0.01,
    gzip_output_files: bool = False,
    length: int = 20,
    trim_n: bool = False,
    report_file: bool = True,
    polyA: bool = False,
    implicon: bool = False,
    retain_unpaired: bool = True,
    length_1: int = 35,
    length_2: int = 35,
) -> LatchDir:
    # local dir to write output files to
    local_dir = "trim_galore_out"

    # input / output arguments
    _cmd = [
        "./TrimGalore-0.6.10/trim_galore",
        input_forward.local_path,
        input_reverse.local_path,
        "--paired",
        "--output_dir",
        local_dir,
    ]

    if base_out:
        _cmd.extend(["--basename", base_out])

    if gzip_output_files:
        _cmd.append("--gzip")
    else:
        _cmd.append("--dont_gzip")

    if retain_unpaired:
        _cmd.append("--retain_unpaired")

    # quality arguments
    _cmd.extend(
        [
            "--quality",
            str(quality),
            base_quality_encoding.value,
            "--length",
            str(length),
            "--length_1",
            str(length_1),
            "--length_2",
            str(length_2),
        ]
    )

    if max_length:
        _cmd.extend(["--max_length", str(max_length)])

    if max_n:
        _cmd.extend(["--max_n", str(max_n)])

    if trim_n:
        _cmd.append("--trim-n")

    # arguments for handling adapters
    if adapter_sequence != adapter_sequence.auto:
        _cmd.append(adapter_sequence.value)

    if error_rate:
        _cmd.extend(["-e", str(error_rate)])

    _cmd.extend(["--stringency", str(stringency)])

    if adapter:
        _cmd.extend(["--adapter", adapter])

    if adapter2:
        _cmd.extend(["--adapter2", adapter2])

    if consider_already_trimmed:
        _cmd.extend(["--consider_already_trimmed", str(consider_already_trimmed)])

    # reporting arguments
    if fastqc:
        _cmd.append("--fastqc")

    if fastqc_args:
        _cmd.extend(["--fastqc_args", f'"{fastqc_args}"'])

    if not report_file:
        _cmd.append("--no_report_file")

    # Misc. arguments
    if hardtrim5:
        _cmd.extend(["--hardtrim5", str(hardtrim5)])

    if hardtrim3:
        _cmd.extend(["--hardtrim3", str(hardtrim3)])

    if clip_R1:
        _cmd.extend(["--clip_R1", str(clip_R1)])

    if clip_R2:
        _cmd.extend(["--clip_R2", str(clip_R2)])

    if polyA:
        _cmd.append("--polyA")

    if implicon:
        _cmd.append("--implicon")

    if three_prime_clip_R1:
        _cmd.extend(["--three_prime_clip_R1", str(three_prime_clip_R1)])

    if three_prime_clip_R2:
        _cmd.extend(["--three_prime_clip_R2", str(three_prime_clip_R2)])

    subprocess.run(_cmd, stdout=subprocess.PIPE)

    if output_directory is not None:
        latch_out_location = _fmt_dir(output_directory.remote_source)
    else:
        latch_out_location = f"latch:///Trim Galore Output/"

    return LatchDir(local_dir, latch_out_location)


@workflow(metadata)
def trim_galore(
    input_forward: LatchFile,
    input_reverse: LatchFile,
    base_out: Optional[str],
    output_directory: Optional[LatchDir],
    fastqc_args: Optional[str],
    adapter: Optional[str],
    adapter2: Optional[str],
    consider_already_trimmed: Optional[int],
    max_length: Optional[int],
    max_n: Optional[float],
    clip_R1: Optional[int],
    clip_R2: Optional[int],
    three_prime_clip_R1: Optional[int],
    three_prime_clip_R2: Optional[int],
    hardtrim5: Optional[int],
    hardtrim3: Optional[int],
    quality: int = 20,
    base_quality_encoding: BaseQualityEncoding = BaseQualityEncoding.phred33,
    fastqc: bool = True,
    adapter_sequence: AdapterSequence = AdapterSequence.auto,
    stringency: int = 1,
    error_rate: float = 0.01,
    gzip_output_files: bool = False,
    length: int = 20,
    trim_n: bool = False,
    report_file: bool = True,
    polyA: bool = False,
    implicon: bool = False,
    retain_unpaired: bool = True,
    length_1: int = 35,
    length_2: int = 35,
) -> LatchDir:
    """
    Quality and adapter trim raw paired-end reads
    ----
    # Trim Galore
    ## About
    Trim Galore! is a wrapper script for CutAdapt and FastQC to automate quality and adapter trimming as well as quality control.
    We provide support for processing of paired-end NGS data.
    <br /><br />
    ## Options
    Trim Galore! has many options, which have been organized into distinct groups here.
    <br /><br />
    ### Input / Output
    - Input forward
    - Input reverse
    - Basename for output files
        - Use preferred string as the basename for output files, instead of deriving the filenames from the input files.
        - preferred_string_val_1.fq(.gz) and preferred_string_val_2.fq(.gz) for paired-end data
    - Output directory name
        - If specified all output will be written to this directory instead of Trim Galore Output.
    - GZip output
        - Compress the output file with GZIP.
        - If the input files are GZIP-compressed the output files will automatically be GZIP compressed as well.
    - Retain unpaired reads
        - If only one of the two paired-end reads became too short, the longer
        read will be written to either '.unpaired_1.fq' or '.unpaired_2.fq'
        output files.
    <br /><br />
    ### Quality 
    - Base quality encoding
        - Specify if the Phred33 or Phred64 scoring schema is used
    - Quality threshold
        - Trim low-quality ends from reads in addition to adapter removal.
        - The algorithm is the same as the one used by BWA
            - (Subtract INT from all qualities; compute partial sums from all indices
                to the end of the sequence; cut sequence at the index at which the sum is
                minimal
    - Read length cutoff
        - Discard reads that became shorter than length INT because of either quality or adapter trimming.
        - A value of '0' effectively disables this behaviour
    - Unpaired single-end read 1 length cutoff
        - Unpaired single-end read length cutoff needed for read 1 to be written to '.unpaired_1.fq' output file.
    - Unpaired single-end read 2 length cutoff
        - Unpaired single-end read length cutoff needed for read 2 to be written to '.unpaired_2.fq' output file.        
    - Max Ns in a read
        - The total number of Ns a read may contain before it will be removed altogether.
    - Trim Ns at ends
        - Removes Ns from either side of the read.
    - Max length
        - Discard reads that are longer than inputted argument in base pairs after trimming.
        - This is only advised for smallRNA sequencing to remove non-small RNA sequences
    <br /><br />
    ### Adapters
    - Adapter sequence to be trimmed from precurated set
        - Select from a precurated list of adapters to look for in sequences.
        - Options include: auto detect, illumina, stranded_illumina, nextera, and small_rna
    - Error rate
        - Maximum allowed error rate
        - Error rate is defined as the no. of errors divided by the length of the matching region.
    - Stringency
        - Overlap with adapter sequence required to trim a sequence
    - Adapter sequence to be trimmed
        - Adapter sequence to be trimmed
    - Adapter sequence to be trimmed for read 2 of paired-end files
    - Consider already trimmed
        - During adapter auto-detection, the limit set by argument allows the user to 
        set a threshold up to which the file is considered already adapter-trimmed
        - If no adapter sequence exceeds this threshold, no additional adapter trimming will be performed
    <br /><br />
    ### Reporting
    - Enable FastQC
        - Run FastQC in the default mode on the FastQ file once trimming is complete
    - Additional FastQC Argument
        - Pass extra argument to FastQC.
    - Generate report files
    <br /><br />
    ### Miscellaneous
    - Hard-trim 5'-end
        - Instead of performing adapter-/quality trimming, this option will simply hard-trim sequences
        to <int> bp at the 5'-end.
        - Once hard-trimming of files is complete, Trim Galore will exit.
        - Hard-trimmed output files will end in .<int>_5prime.fq(.gz)
    - Hard-trim 3'-end
        - Same behavior as Hard-trim 5'-end
    - Trim 5' end of read 1
        - Instructs Trim Galore to remove <int> bp from the 5' end of read 1.
        - Equivalent to clip_R1 argument
    - Trim 5' end of read 2
        - Instructs Trim Galore to remove <int> bp from the 5' end of read 2
        - Equivalent to clip_R2 argument
    - Trim 3' end of read 1
        - Instructs Trim Galore to remove <int> bp from the 3' end of read 1 AFTER adapter/quality trimming has been performed
        - Equivalent to three_prime_clip_R1 argument
    - Trim 3' end of read 2
        - Instructs Trim Galore to remove <int> bp from the 3' end of read 2 AFTER adapter/quality trimming has been performed
        - Equivalent to three_prime_clip_R2 argument
    - Remove PolyA tails
        - This is a new, still experimental, trimming mode to identify and remove poly-A tails from sequences
    - Implicon
        - This is a special mode of operation for paired-end data, such as required for the IMPLICON method, where a UMI sequence
        is getting transferred from the start of Read 2 to the readID of both reads. 

    Citation: *Trim Galore!*. [Babraham Bioinformatics](https://www.bioinformatics.babraham.ac.uk/projects/trim_galore/).
    """

    return trim_reads_task(
        input_forward=input_forward,
        input_reverse=input_reverse,
        base_out=base_out,
        output_directory=output_directory,
        fastqc_args=fastqc_args,
        adapter=adapter,
        adapter2=adapter2,
        consider_already_trimmed=consider_already_trimmed,
        max_length=max_length,
        max_n=max_n,
        clip_R1=clip_R1,
        clip_R2=clip_R2,
        three_prime_clip_R1=three_prime_clip_R1,
        three_prime_clip_R2=three_prime_clip_R2,
        hardtrim5=hardtrim5,
        hardtrim3=hardtrim3,
        quality=quality,
        base_quality_encoding=base_quality_encoding,
        fastqc=fastqc,
        adapter_sequence=adapter_sequence,
        stringency=stringency,
        error_rate=error_rate,
        gzip_output_files=gzip_output_files,
        length=length,
        trim_n=trim_n,
        report_file=report_file,
        polyA=polyA,
        implicon=implicon,
        retain_unpaired=retain_unpaired,
        length_1=length_1,
        length_2=length_2,
    )


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    trim_galore,
    "Small test data",
    {
        "input_forward": LatchFile(
            "s3://latch-public/test-data/1656/SRR23924051_1_subset.fq"
        ),
        "input_reverse": LatchFile(
            "s3://latch-public/test-data/1656/SRR23924051_2_subset.fq"
        ),
    },
)
