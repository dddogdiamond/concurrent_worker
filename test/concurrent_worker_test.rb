require "test_helper"

class ConcurrentWorkerTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::ConcurrentWorker::VERSION
  end

end

class WorkBlockParamTest < Minitest::Test
  def setup
    @type = :thread
    @worker = ConcurrentWorker::Worker.new(type: @type)
  end

  def test_workblock_1arg_1param
    #
    # workblock :  1arg 1param
    #
    result = nil
    @worker.req(3) do |n|
      result = n
    end
    @worker.join
    assert_equal [@type,3],[@type,result]
  end

  def test_workblock_2arg_2param
    #
    # workblock : 2arg 2param
    #
    result = nil
    @worker.req(3,5) do |n0,n1|
      result = [n0,n1]
    end
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
    
  def test_workblock_2arg_1param
    #
    # workblock : 2arg 1param
    #
    result = nil
    @worker.req(3,5) do |param|
      result = param
    end
    @worker.join
    assert_equal [@type,3],[@type,result]
  end
  
  def test_workblock_1arg_astrparam
    #
    # workblock : 1arg *param
    #
    result = nil
    @worker.req(3) do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
    
  def test_workblock_2arg_astrparam
    #
    # workblock : 2param *arg
    #
    result = nil
    @worker.req(3,5) do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_astrarg_astrparam
    #
    # workblock : *arg *param
    #
    result = nil
    arg = [3,5]
    @worker.req(*arg) do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_arrayarg_param
    #
    # workblock : arrayarg param
    #
    result = nil
    array = [3,5]
    @worker.req(array) do |param|
      result = param
    end
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_arrayarg_astrparam
    #
    # workblock : arrayarg *param
    #
    result = nil
    array = [3,5]
    @worker.req(array) do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end

  def test_workblock_nilarg_param
    #
    # workblock : nilarg param
    #
    result = nil
    @worker.req(nil) do |param|
      result = param
    end
    @worker.join
    assert_equal [@type,nil],[@type,result]
  end
  
  def test_workblock_nilarg_astrparam
    #
    # workblock : nilarg *param
    #
    result = nil
    @worker.req(nil) do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[nil]],[@type,result]
  end
  
  def test_workblock_noarg_param
    #
    # workblock : noarg param
    #
    result = nil
    @worker.req() do |param|
      result = param
    end
    @worker.join
    assert_equal [@type,nil],[@type,result]
  end

  def test_workblock_noarg_astrparam
    #
    # workblock : noarg param
    #
    result = nil
    @worker.req() do |*param|
      result = param
    end
    @worker.join
    assert_equal [@type,[]],[@type,result]
  end
end


class SetWorkBlockParamTest < Minitest::Test
  def setup
    @type = :thread
    @worker = ConcurrentWorker::Worker.new(type: @type)
  end

  def test_workblock_1arg_1param
    #
    # workblock :  1arg 1param
    #
    result = nil
    @worker.set_block(:work_block) do |n|
      result = n
    end
    @worker.req(3)
    @worker.join
    assert_equal [@type,3],[@type,result]
  end

  def test_workblock_2arg_2param
    #
    # workblock : 2arg 2param
    #
    result = nil
    @worker.set_block(:work_block) do |n0,n1|
      result = [n0,n1]
    end
    @worker.req(3,5)
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
    
  def test_workblock_2arg_1param
    #
    # workblock : 2arg 1param
    #
    result = nil
    @worker.set_block(:work_block) do |param|
      result = param
    end
    @worker.req(3,5)
    @worker.join
    assert_equal [@type,3],[@type,result]
  end
  
  def test_workblock_1arg_astrparam
    #
    # workblock : 1arg *param
    #
    result = nil
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req(3)
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
    
  def test_workblock_2arg_astrparam
    #
    # workblock : 2param *arg
    #
    result = nil
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req(3,5)
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_astrarg_astrparam
    #
    # workblock : *arg *param
    #
    result = nil
    arg = [3,5]
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req(*arg)
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_arrayarg_param
    #
    # workblock : arrayarg param
    #
    result = nil
    array = [3,5]
    @worker.set_block(:work_block) do |param|
      result = param
    end
    @worker.req(array)
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_workblock_arrayarg_astrparam
    #
    # workblock : arrayarg *param
    #
    result = nil
    array = [3,5]
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req(array)
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end



  def test_workblock_nilarg_param
    #
    # workblock : nilarg param
    #
    result = nil
    @worker.set_block(:work_block) do |param|
      result = param
    end
    @worker.req(nil)
    @worker.join
    assert_equal [@type,nil],[@type,result]
  end
  
  def test_workblock_nilarg_astrparam
    #
    # workblock : nilarg *param
    #
    result = nil
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req(nil)
    @worker.join
    assert_equal [@type,[nil]],[@type,result]
  end
  
  def test_workblock_noarg_param
    #
    # workblock : noarg param
    #
    result = nil
    @worker.set_block(:work_block) do |param|
      result = param
    end
    @worker.req()
    @worker.join
    assert_equal [@type,nil],[@type,result]
  end

  def test_workblock_noarg_astrparam
    #
    # workblock : noarg param
    #
    result = nil
    @worker.set_block(:work_block) do |*param|
      result = param
    end
    @worker.req()
    @worker.join
    assert_equal [@type,[]],[@type,result]
  end
  

end


class CallbackParamTest < Minitest::Test
  def setup
    @type = :thread
  end
  
  def test_callback_1ret_1param
    #
    # callback :  1ret 1param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      3
    end
    @worker.add_callback do |n|
      result = n
    end
    @worker.req
    @worker.join
    assert_equal [@type,3],[@type,result]
  end

  def test_callback_1elem_array_ret_1param
    #
    # callback :  [1ret] 1param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      [3]
    end
    @worker.add_callback do |n|
      result = n
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
  
  def test_callback_2ret_2param
    #
    # callback : [2ret] 2param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      [3,5]
    end
    @worker.add_callback do |n0,n1|
      result = [n0,n1]
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
    
  def test_callback_2ret_1param
    #
    # callback : [2ret] 1param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      [3,5]
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
  
  def test_callback_1ret_astrparam
    #
    # callback : 1ret *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      3
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
    
  def test_callback_2ret_astrparam
    #
    # callback : [2ret] *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      [3,5]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end


  def test_callback_astrret_astrparam
    #
    # callback : [*ret] *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      ret = [3,5]
      [*ret]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end


  def test_callback_arrayret_param
    #
    # callback : arrayret param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      array = [3,5]
      array
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end

  def test_callback_arrayarrayret_param
    #
    # callback : [arrayret] param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      array = [3,5]
      [array]
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end
  
  def test_callback_arrayret_astrparam
    #
    # callback : arrayret *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      array = [3,5]
      array
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end

  def test_callback_arrayarrayret_astrparam
    #
    # callback : arrayarrayret *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      array = [3,5]
      [array]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[[3,5]]]],[@type,result]
  end
  

  

  def test_callback_nilret_param
    #
    # callback : nilret param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      nil
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,nil],[@type,result]
  end
  
  def test_callback_nilret_astrparam
    #
    # callback : nilret *param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      nil
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[nil]],[@type,result]
  end
  
  def test_callback_noret_param
    #
    # callback : noret param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      []
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[]],[@type,result]
  end

  def test_callback_noret_astrparam
    #
    # callback : noret param
    #
    result = nil
    @worker = ConcurrentWorker::Worker.new(type: @type) do
      []
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[]]],[@type,result]
  end
end


class SetWorkBlockCallbackParamTest < Minitest::Test
  def setup
    @type = :thread
    @worker = ConcurrentWorker::Worker.new(type: @type)
  end
  
  def test_callback_1ret_1param
    #
    # callback :  1ret 1param
    #
    result = nil
    @worker.set_block(:work_block) do
      3
    end
    @worker.add_callback do |n|
      result = n
    end
    @worker.req
    @worker.join
    assert_equal [@type,3],[@type,result]
  end

  def test_callback_1elem_array_ret_1param
    #
    # callback :  [1ret] 1param
    #
    result = nil
    @worker.set_block(:work_block)  do
      [3]
    end
    @worker.add_callback do |n|
      result = n
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
  
  def test_callback_2ret_2param
    #
    # callback : [2ret] 2param
    #
    result = nil
    @worker.set_block(:work_block)  do
      [3,5]
    end
    @worker.add_callback do |n0,n1|
      result = [n0,n1]
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
    
  def test_callback_2ret_1param
    #
    # callback : [2ret] 1param
    #
    result = nil
    @worker.set_block(:work_block)  do
      [3,5]
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result]
  end
  
  def test_callback_1ret_astrparam
    #
    # callback : 1ret *param
    #
    result = nil
    @worker.set_block(:work_block)  do
      3
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3]],[@type,result]
  end
    
  def test_callback_2ret_astrparam
    #
    # callback : [2ret] *param
    #
    result = nil
    @worker.set_block(:work_block)  do
      [3,5]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end


  def test_callback_astrret_astrparam
    #
    # callback : [*ret] *param
    #
    result = nil
    @worker.set_block(:work_block)  do
      ret = [3,5]
      [*ret]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end


  def test_callback_arrayret_param
    #
    # callback : arrayret param
    #
    result = nil
    @worker.set_block(:work_block)  do
      array = [3,5]
      array
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[3,5]],[@type,result] # can't be [3,5]
  end

  def test_callback_arrayarrayret_param
    #
    # callback : [arrayret] param
    #
    result = nil
    @worker.set_block(:work_block)  do
      array = [3,5]
      [array]
    end
    @worker.add_callback do |param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end
  
  def test_callback_arrayret_astrparam
    #
    # callback : arrayret *param
    #
    result = nil
    @worker.set_block(:work_block)  do
      array = [3,5]
      array
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[3,5]]],[@type,result]
  end

  def test_callback_arrayarrayret_astrparam
    #
    # callback : [arrayret] *param
    #
    result = nil
    @worker.set_block(:work_block)  do
      array = [3,5]
      [array]
    end
    @worker.add_callback do |*param|
      result = param
    end
    @worker.req
    @worker.join
    assert_equal [@type,[[[3,5]]]],[@type,result]
  end

end

class ProcessCallbackParamTest < CallbackParamTest
  def setup
    @type = :process
  end  
end

class ProcessSetWorkBlockCallbackParamTest < SetWorkBlockCallbackParamTest
  def setup
    @type = :process
    @worker = ConcurrentWorker::Worker.new(type: @type)
  end
end


class WorkerPoolWorkBlockParamTest < WorkBlockParamTest
  def setup
    @type = :thread
    @worker = ConcurrentWorker::WorkerPool.new(type: @type)
  end
end


class WorkerPoolSetWorkBlockParamTest < SetWorkBlockParamTest
  def setup
    @type = :thread
    @worker = ConcurrentWorker::WorkerPool.new(type: @type)
  end
end

class WorkerPoolSetWorkBlockCallbackParamTest < SetWorkBlockCallbackParamTest
  def setup
    @type = :process
    @worker = ConcurrentWorker::WorkerPool.new(type: @type)
  end
end


