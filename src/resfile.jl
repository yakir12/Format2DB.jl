using MAT, DelimitedFiles, SparseArrays

ncol(io) = mapreduce(!isempty, +, eachcol(read(io, "xdata"))) # only works well if the empty column is last

function savepixels(pixelfolder, resfile)
    matopen(resfile) do io
        n = ncol(io)
        ids = Vector{UUID}(undef, n)
        for i in 1:n
            x, y, t = getcoordinates(io, i)
            # @assert !isempty(x) "no coordinates in column $i"
            id = uuid1()
            writedlm(joinpath(pixelfolder, "$id.csv"), zip(x, y, t))
            ids[i] = id
        end
        ids
    end
end

function getcoordinates(io, i)
    _x = read(io, "xdata")[:, i]
    x = nonzeros(_x)
    y = nonzeros(read(io, "ydata")[:,i])
    fr = read(io, "status")["FrameRate"]
    t = length(x) == 1 ? findfirst(!iszero, _x)/fr : findall(!iszero, _x)./fr
    return x, y, t
end



