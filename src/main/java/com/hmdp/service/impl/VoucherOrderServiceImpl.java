package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀还未开始");
        }
        // 判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 已经结束
            return Result.fail("秒杀已经结束");
        }
        // 判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        // 减小锁的粒度，只锁住当前用户的操作
        // 但是在方法内锁，锁释放后事务还没提交，其他线程还是可以进来，所以要在方法外锁
        // toString()每次会生成一个新的字符串对象，所以这里要用intern()方法获取字符串常量池中的字符串对象
        synchronized (userId.toString().intern()) {
            // 事务生效是调用的代理对象，所以这里应该调用的是代理对象；直接调用目标对象会导致事务失效
            // return createVoucherOrder(voucherId);

            // 需要导入spring-aop包，开启AOP代理
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 一人一单
        Long userId = UserHolder.getUser().getId();

        // 查询订单
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            // 已经抢购过了
            return Result.fail("已经抢购过了");
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                // set stock = stock - 1
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                // 乐观锁, where id = ? and stock = ?
                // .eq("stock", voucher.getStock())
                // 这样会在高并发下导致大量的更新失败，每次并发都要判断有些多余
                // 设置判断条件为stock > 0就会好很多
                // 乐观锁优化，where id = ? and stock > 0
                .gt("stock", 0)
                .update();
        if (!success) {
            // 扣减失败，库存不足
            return Result.fail("库存不足");
        }
        // 创建订单
        VoucherOrder order = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        order.setId(orderId);
        // 用户id
        order.setUserId(userId);
        // 优惠券id
        order.setVoucherId(voucherId);
        save(order);
        // 返回订单ID
        return Result.ok(orderId);
    }
}
